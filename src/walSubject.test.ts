import { Pool } from "pg";
import { describe, test, expect, beforeAll, mock } from "bun:test";
import invariant from "tiny-invariant";
import {
  GetObservableEventType,
  getKyselyChanges,
  listWalSlots,
} from "./walSubject";
import { Kysely, PostgresDialect } from "kysely";

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
  widgets: {
    id: number;
    kind: string;
    created_at: Date;
    updated_at: Date;
  };
  other_table_on_public: {
    id: number;
    kind: string;
    created_at: Date;
    updated_at: Date;
  };
  "schema_two.widgets": {
    id: number;
    kind: string;
    created_at: Date;
    updated_at: Date;
  };
  "schema_two.other_table_on_schema_two": {
    id: number;
    kind: string;
    created_at: Date;
    updated_at: Date;
  };
};

describe("walSubject", () => {
  beforeAll(async () => {
    await testPool.query(`
      drop table if exists widgets;
      create table widgets (
        id serial primary key,
        kind text not null,
        created_at timestamp not null default now(),
        updated_at timestamp not null default now()
      );

      drop table if exists other_table_on_public;
      create table other_table_on_public (
        id serial primary key,
        kind text not null,
        created_at timestamp not null default now(),
        updated_at timestamp not null default now()
      );

      drop schema if exists schema_two cascade;
      create schema schema_two;
      create table schema_two.widgets (
        id serial primary key,
        kind text not null,
        created_at timestamp not null default now(),
        updated_at timestamp not null default now()
      );

      create table schema_two.other_table_on_schema_two (
        id serial primary key,
        kind text not null,
        created_at timestamp not null default now(),
        updated_at timestamp not null default now()
      );
    `);
  });

  test("getKyselyChanges creates a temporary slot and teardown removes it", async () => {
    const slotId = "basic_create_test";
    const { subject, teardown } = await getKyselyChanges(
      testPool,
      db,
      ["widgets"],
      undefined,
      { slotId }
    );

    const slots = await listWalSlots(testPool);
    const mySlot = slots.find((s) => s.slot_name === `app_slot_${slotId}`);
    expect(mySlot).not.toBeUndefined();

    await teardown();

    const slotsAfterTeardown = await listWalSlots(testPool);
    const mySlotAfterTeardown = slotsAfterTeardown.find(
      (s) => s.slot_name === `app_slot_${slotId}`
    );
    expect(mySlotAfterTeardown).toBeUndefined();
  });

  test("can get changes from the subject", async () => {
    const slotId = "basic_get_changes_test";
    const { subject, teardown } = await getKyselyChanges(
      testPool,
      db,
      ["widgets"],
      undefined,
      {
        slotId,
      }
    );

    const listener = mock(() => {});

    const subscription = subject.subscribe({
      next: listener,
    });

    await testPool.query("insert into widgets (kind) values ($1)", [
      "baseball",
    ]);

    await sleep(100);

    await teardown();
    subscription.unsubscribe();

    expect(listener).toHaveBeenCalled();
  });

  test("changes from the subject follow a predictable shape", async () => {
    const slotId = "basic_get_changes_shape_test";
    const { subject, teardown } = await getKyselyChanges(
      testPool,
      db,
      ["widgets"],
      undefined,
      {
        slotId,
      }
    );

    let event: Parameters<typeof subject.next>[0] | undefined;
    const listener = mock((change) => {
      event = change;
    });

    const subscription = subject.subscribe((change) => {
      listener(change);
    });

    await testPool.query("insert into widgets (kind) values ($1)", [
      "baseball",
    ]);

    await sleep(100);

    await teardown();
    subscription.unsubscribe();

    expect(listener).toHaveBeenCalled();
    invariant(event);
    expect(event.event).toBe("insert");
    expect(event.table).toBe("widgets");
  });

  test("only get changes from the requested tables", async () => {
    const slotId = "basic_get_changes_exclude_tables_test";
    const { subject, teardown } = await getKyselyChanges(
      testPool,
      db,
      ["widgets"],
      undefined,
      {
        slotId,
      }
    );

    const changes: GetObservableEventType<typeof subject>[] = [];
    const listener = mock((change) => {
      changes.push(change);
    });

    const subscription = subject.subscribe(listener);

    await testPool.query("insert into widgets (kind) values ($1)", [
      "baseball",
    ]);

    await testPool.query(
      "insert into other_table_on_public (kind) values ($1)",
      ["basketball"]
    );

    await sleep(100);

    await teardown();
    subscription.unsubscribe();

    expect(listener).toHaveBeenCalledTimes(1);
    const basketballChange = changes.find(
      // To verify that typescript is excluding this type, remove as any
      // and get the type error
      (change) => (change.table as any) === "other_table_on_public"
    );
    expect(basketballChange).toBeUndefined();
  });

  test("update payload follows a predictable shape", async () => {
    const slotId = "update_get_changes_shape_test";

    const insertResult = await testPool.query(
      "insert into widgets (kind) values ($1) returning id",
      ["baseball"]
    );
    const insertedWidgetId = insertResult.rows[0].id;

    const { subject, teardown } = await getKyselyChanges(
      testPool,
      db,
      ["widgets"],
      undefined,
      {
        slotId,
      }
    );

    await testPool.query("update widgets set kind = $1 where id = $2", [
      "basketball",
      insertedWidgetId,
    ]);

    let event: Parameters<typeof subject.next>[0] | undefined;
    const listener = mock((change) => {
      event = change;
    });

    const subscription = subject.subscribe((change) => {
      listener(change);
    });

    await sleep(100);

    await teardown();
    subscription.unsubscribe();

    expect(listener).toHaveBeenCalled();
    invariant(event);
    expect(event.event).toBe("update");
    expect(event.table).toBe("widgets");

    invariant(event.event === "update");

    expect(event.row.kind).toBe("basketball");
  });

  test("delete payload follows a predictable shape and is typed to only include primary keys", async () => {
    const slotId = "delete_get_changes_shape_test";

    const insertResult = await testPool.query(
      "insert into widgets (kind) values ($1) returning id",
      ["baseball"]
    );
    const insertedWidgetId = insertResult.rows[0].id;

    const { subject, teardown } = await getKyselyChanges(
      testPool,
      db,
      ["widgets"],
      undefined,
      {
        slotId,
      }
    );

    await testPool.query("delete from widgets where id = $1", [
      insertedWidgetId,
    ]);

    let event: Parameters<typeof subject.next>[0] | undefined;
    const listener = mock((change) => {
      event = change;
    });

    const subscription = subject.subscribe((change) => {
      listener(change);
    });

    await sleep(100);

    await teardown();
    subscription.unsubscribe();

    expect(listener).toHaveBeenCalled();
    invariant(event);
    expect(event.event).toBe("delete");
    expect(event.table).toBe("widgets");

    invariant(event.event === "delete");

    expect(event.identity.id).toBe(insertedWidgetId);
    // To verify that delete only include primary keys, remove as any
    // and see the error
    expect((event.identity as any).kind).toBe(undefined);
  });

  test("delete primary key override changes the type of identity", async () => {
    const slotId = "delete_primary_key_override_test";

    // Assuming 'widgets' table has a composite primary key [id, kind]
    const primaryKeyOverride = { widgets: ["id", "kind"] } as const;

    const insertResult = await testPool.query(
      "insert into widgets (kind) values ($1) returning id, kind",
      ["slinky"]
    );
    const insertedWidget = insertResult.rows[0];

    const { subject, teardown } = await getKyselyChanges(
      testPool,
      db,
      ["widgets"],
      primaryKeyOverride,
      {
        slotId,
      }
    );

    await testPool.query("delete from widgets where id = $1 and kind = $2", [
      insertedWidget.id,
      insertedWidget.kind,
    ]);

    let event: Parameters<typeof subject.next>[0] | undefined;
    const listener = mock((change) => {
      event = change;
    });

    const subscription = subject.subscribe((change) => {
      listener(change);
    });

    await sleep(100);

    await teardown();
    subscription.unsubscribe();

    expect(listener).toHaveBeenCalled();
    invariant(event);
    expect(event.event).toBe("delete");
    expect(event.table).toBe("widgets");

    invariant(event.event === "delete");

    // Check that the identity includes both primary keys
    expect(event.identity.id).toBe(insertedWidget.id);
    // Expect it to be undefined, but expect Typescript to allow us to lie about it being defined
    expect(event.identity.kind).toBeUndefined();
  });
});
