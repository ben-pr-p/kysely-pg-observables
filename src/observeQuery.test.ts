import { describe, test, expect, mock, beforeAll } from "bun:test";
import { observeQuery } from "./observeQuery";
import { getKyselyChanges } from "./walSubject";
import { Pool } from "pg";
import { ColumnType, Kysely, PostgresDialect } from "kysely";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const testPool = new Pool({
  connectionString:
    process.env.DATABASE_URL ||
    "postgres://postgres:postgres@localhost:7432/postgres",
});

const dialect = new PostgresDialect({
  pool: testPool,
});

const db = new Kysely<TestDatabase>({
  dialect,
});

type TestDatabase = {
  cards: {
    id: ColumnType<number, undefined, undefined>;
    kind: string;
    deck_id: ColumnType<
      number | undefined,
      number | undefined,
      number | undefined
    >;
    created_at: ColumnType<Date, Date | undefined, Date | undefined>;
    updated_at: ColumnType<Date, Date | undefined, Date | undefined>;
  };
  decks: {
    id: ColumnType<number, undefined, undefined>;
    name: string;
    created_at: ColumnType<Date, Date | undefined, Date | undefined>;
    updated_at: ColumnType<Date, Date | undefined, Date | undefined>;
  };
  ignore_this_table: {
    id: ColumnType<number, undefined, undefined>;
    kind: string;
    created_at: Date;
    updated_at: Date;
  };
};

describe("observeQuery", () => {
  beforeAll(async () => {
    await testPool.query(`
      drop table if exists decks cascade;
      create table decks (
        id serial primary key,
        name text not null,
        created_at timestamp not null default now(),
        updated_at timestamp not null default now()
      );

      drop table if exists cards;
      create table cards (
        id serial primary key,
        kind text not null,
        deck_id integer references decks (id),
        created_at timestamp not null default now(),
        updated_at timestamp not null default now()
      );

      drop table if exists ignore_this_table;
      create table ignore_this_table (
        id serial primary key,
        kind text not null,
        created_at timestamp not null default now(),
        updated_at timestamp not null default now()
      );
    `);
  });

  test("query re-runs if the handler.insert returns true", async () => {
    const uniqueKindForThisTest = Math.random().toString();
    const changes = await getKyselyChanges(testPool, db, ["cards"], undefined);

    const nextListener = mock(() => {});

    const observable = observeQuery(
      changes,
      () => db.selectFrom("cards").selectAll().execute(),
      {
        cards: {
          insert(row) {
            return row.kind === uniqueKindForThisTest;
          },
        },
      }
    );

    const subscription = observable.subscribe({
      next: nextListener,
    });

    await db
      .insertInto("cards")
      .values({ kind: uniqueKindForThisTest })
      .returning("id")
      .execute();

    await sleep(100);

    subscription.unsubscribe();
    await changes.teardown();

    expect(nextListener).toHaveBeenCalledTimes(2);
  });

  test("query does not re-run if the handler.insert returns false", async () => {
    const uniqueKindForThisTest = Math.random().toString();
    const changes = await getKyselyChanges(testPool, db, ["cards"], undefined);

    const nextListener = mock(() => {});

    const observable = observeQuery(
      changes,
      () => db.selectFrom("cards").selectAll().execute(),
      {
        cards: {
          insert(row) {
            return false;
          },
        },
      }
    );

    const subscription = observable.subscribe({
      next: nextListener,
    });

    await db
      .insertInto("cards")
      .values({ kind: uniqueKindForThisTest })
      .returning("id")
      .execute();

    await sleep(100);

    subscription.unsubscribe();
    await changes.teardown();

    expect(nextListener).toHaveBeenCalledTimes(1); // Should be called only once initially
  });

  test("query only re-runs once if we get two invalidation events while the query is running", async () => {
    const uniqueKindForThisTest = Math.random().toString();
    const changes = await getKyselyChanges(testPool, db, ["cards"], undefined, {
      // Fast poll interval for shorter test
      pollInterval: 5,
    });

    const nextListener = mock(() => {});

    const observable = observeQuery(
      changes,
      async () => {
        const result = await db
          .selectFrom("cards")
          .selectAll()
          .select((qb) => [qb.fn("pg_sleep", [qb.val(0.1)]).as("sleep")])
          .execute();

        return result;
      },
      {
        cards: {
          insert(row) {
            return row.kind === uniqueKindForThisTest;
          },
        },
      }
    );

    const subscription = observable.subscribe({
      next: nextListener,
    });

    await sleep(10);

    await db
      .insertInto("cards")
      .values({ kind: uniqueKindForThisTest })
      .returning("id")
      .execute();

    await sleep(10);

    // Query takes 2 seconds to run, so it still isn't done by this point
    // after being queued in the first time
    // As a result, since the above invalidation event is still queued
    // We should expect just one more run to fire from both the above
    // and this current insert
    await db
      .insertInto("cards")
      .values({ kind: uniqueKindForThisTest })
      .returning("id")
      .execute();

    // Give it enough time to let the query run twice
    await sleep(1000);

    subscription.unsubscribe();
    await changes.teardown();

    expect(nextListener).toHaveBeenCalledTimes(2);
  });

  test("can make decisions based on the lastResult", async () => {
    const uniqueKindForThisTest = Math.random().toString();
    const changes = await getKyselyChanges(testPool, db, ["cards"], undefined);

    const nextListener = mock(() => {});

    await db.deleteFrom("cards").execute();

    await db
      .insertInto("cards")
      .values({ kind: uniqueKindForThisTest })
      .execute();

    await db.insertInto("cards").values({ kind: "dont include me!" }).execute();

    const observable = observeQuery(
      changes,
      () =>
        db
          .selectFrom("cards")
          .selectAll()
          .where("kind", "=", uniqueKindForThisTest)
          .execute(),
      {
        cards: {
          insert(row) {
            return true;
          },
          update(row, lastResult) {
            return lastResult.some((card) => card.id === row.id);
          },
          delete(row, lastResult) {
            return true;
          },
        },
      }
    );

    await sleep(100);

    const subscription = observable.subscribe({
      next: nextListener,
    });

    // Update a card that should trigger a rerun
    await db
      .updateTable("cards")
      .set({ updated_at: new Date() })
      .where("kind", "=", uniqueKindForThisTest)
      .execute();

    await sleep(100);

    // Update a card that shouldn't trigger a rerun
    await db
      .updateTable("cards")
      .set({ updated_at: new Date() })
      .where("kind", "!=", uniqueKindForThisTest)
      .execute();

    await sleep(100);

    subscription.unsubscribe();
    await changes.teardown();

    expect(nextListener).toHaveBeenCalledTimes(2);
  });

  test("can make decisions based on the lastResult asynchronously", async () => {
    const uniqueKindForThisTest = Math.random().toString();
    const changes = await getKyselyChanges(testPool, db, ["cards"], undefined);

    const nextListener = mock(() => {});

    const testDeck = await db
      .insertInto("decks")
      .values({ name: "test deck" })
      .returning("id")
      .executeTakeFirstOrThrow();

    const otherDeck = await db
      .insertInto("decks")
      .values({ name: "other deck" })
      .returning("id")
      .executeTakeFirstOrThrow();

    await db
      .insertInto("cards")
      .values({ kind: uniqueKindForThisTest, deck_id: testDeck.id })
      .execute();

    await db.insertInto("cards").values({ kind: "dont include me!" }).execute();

    const observable = observeQuery(
      changes,
      () =>
        db
          .selectFrom("cards")
          .fullJoin("decks", "cards.deck_id", "decks.id")
          .select((qb) => [qb.fn.count("cards.id").as("card_count")])
          .where("decks.name", "like", "test%")
          .executeTakeFirstOrThrow(),
      {
        cards: {
          async insert(row) {
            if (row.deck_id) {
              const doesNewRowDeckStartWithTest = await db
                .selectFrom("decks")
                .select("name")
                .where("id", "=", row.deck_id)
                .where("name", "like", "test%")
                .executeTakeFirst();

              if (doesNewRowDeckStartWithTest) {
                return true;
              }
            }

            return false;
          },
          update(row, lastResult) {
            return true;
          },
          delete(row, lastResult) {
            return true;
          },
        },
      }
    );

    await sleep(100);

    const subscription = observable.subscribe({
      next: nextListener,
    });

    // Insert a card that should trigger a rerun
    await db
      .insertInto("cards")
      .values({ kind: uniqueKindForThisTest, deck_id: testDeck.id })
      .execute();

    await sleep(100);

    // Insert a card that shouldn't trigger a rerun
    await db
      .insertInto("cards")
      .values({ kind: uniqueKindForThisTest, deck_id: otherDeck.id })
      .execute();

    await sleep(100);

    subscription.unsubscribe();
    await changes.teardown();

    expect(nextListener).toHaveBeenCalledTimes(2);
  });

  test("unsubscribe makes the query stops running", async () => {
    const uniqueKindForThisTest = Math.random().toString();

    const changes = await getKyselyChanges(testPool, db, ["cards"], undefined);

    const runWhenQueryRuns = mock(() => {});

    const observable = observeQuery(
      changes,
      async () => {
        runWhenQueryRuns();

        const result = await db.selectFrom("cards").selectAll().execute();

        return result;
      },
      {
        cards: {
          insert(row, lastResult) {
            return true;
          },
          update(row, lastResult) {
            return true;
          },
          delete(row, lastResult) {
            return true;
          },
        },
      }
    );

    await sleep(100);

    const subscription = observable.subscribe({
      next: () => {},
    });

    await sleep(100);

    // Do an insert that should trigger a rerun
    await db
      .insertInto("cards")
      .values({ kind: uniqueKindForThisTest })
      .execute();

    await sleep(100);
    subscription.unsubscribe();

    // Do another insert that should trigger a rerun
    await db
      .insertInto("cards")
      .values({ kind: uniqueKindForThisTest })
      .execute();

    await sleep(100);

    // And another insert that should trigger a rerun
    await db
      .insertInto("cards")
      .values({ kind: uniqueKindForThisTest })
      .execute();

    await sleep(100);

    subscription.unsubscribe();
    await changes.teardown();

    // One time for the original run, and another time for the first
    // call before .unsubscribe() was called
    // None after that!
    expect(runWhenQueryRuns).toHaveBeenCalledTimes(2);
  });
});
