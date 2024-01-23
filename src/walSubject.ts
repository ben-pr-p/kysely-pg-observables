import { Pool, PoolClient } from "pg";
import invariant from "tiny-invariant";
import { Subject } from "rxjs";
import { Kysely } from "kysely";
import {
  KyselyChanges,
  MinimalKyselyDatabaseTemplate,
  PrimaryKeyOverrideInput,
  SubjectChangeEvent,
} from "./types";

const POLL_INTERVAL_MS = 50;

/**
 * Beware: this may include more than the keys (e.g. if there is no index)
 * Largely copied from: https://github.com/graphile/graphile-engine/blob/v4/packages/lds/src/pg-logical-decoding.ts
 */
interface Keys {
  keynames: Array<string>;
  keytypes: Array<string>;
  keyvalues: Array<unknown>;
}

interface Change {
  // https://github.com/eulerto/wal2json/blob/f81bf7af09324da656be87dfd53d20741c01e1e0/wal2json.c#L957
  schema: string;
  // https://github.com/eulerto/wal2json/blob/f81bf7af09324da656be87dfd53d20741c01e1e0/wal2json.c#L961
  table: string;
}

// https://github.com/eulerto/wal2json/blob/f81bf7af09324da656be87dfd53d20741c01e1e0/wal2json.c#L941-L949
export interface WALInsertChange extends Change {
  kind: "insert";
  // https://github.com/eulerto/wal2json/blob/f81bf7af09324da656be87dfd53d20741c01e1e0/wal2json.c#L969
  columnnames: Array<string>;
  columntypes: Array<string>;
  columnvalues: Array<unknown>;
}

export interface WALUpdateChange extends Change {
  kind: "update";

  // https://github.com/eulerto/wal2json/blob/f81bf7af09324da656be87dfd53d20741c01e1e0/wal2json.c#L973
  columnnames: Array<string>;
  columntypes: Array<string>;
  columnvalues: Array<unknown>;
  // https://github.com/eulerto/wal2json/blob/f81bf7af09324da656be87dfd53d20741c01e1e0/wal2json.c#L992-L1003
  oldkeys: Keys;
}

export interface WALDeleteChange extends Change {
  kind: "delete";
  // https://github.com/eulerto/wal2json/blob/f81bf7af09324da656be87dfd53d20741c01e1e0/wal2json.c#L1009-L1018
  oldkeys: Keys;
}

export type WALChange = WALInsertChange | WALUpdateChange | WALDeleteChange;

interface Payload {
  lsn: string;
  data: {
    change: Array<WALChange>;
  };
}

const toLsnData = ({ lsn, data }: { lsn: string; data: string }): Payload => ({
  lsn,
  data: JSON.parse(data),
});

type WalConfigOptions = {
  slotId?: string;
  pollInterval?: number;
  assumeSchema?: string;
};

const createSlot = async (client: PoolClient, slotName: string) => {
  try {
    await client.query(
      "select pg_catalog.pg_create_logical_replication_slot($1, $2, $3)",
      [slotName, "wal2json", true]
    );
  } catch (e: unknown) {
    invariant(
      e instanceof Error,
      "Expected error thrown from pg_create_logical_replication_slot to be an instance of Error, but got something else"
    );

    if ("code" in e && e.code === "58P01") {
      throw new Error(
        `Couldn't create replication slot, seems you don't have wal2json installed? Error: ${e.message}`
      );
    } else {
      throw e;
    }
  }
};

const dropSlot = async (client: PoolClient, slotName: string) => {
  try {
    await client.query("select pg_catalog.pg_drop_replication_slot($1)", [
      slotName,
    ]);
  } catch (e) {
    // most likely reason for failure is that the slot does not exist, and we're fine!
  }
};

let currentlyGettingChanges = false;
const getChanges = async (
  client: PoolClient,
  slotName: string,
  schema: string,
  onlyIncludeTables: string[],
  changeHandler: (payload: Payload[]) => void
): Promise<void> => {
  // if we're taking longer than POLL_INTERVAL_MS to process hanges, just skip this iteration
  if (currentlyGettingChanges) {
    return;
  }

  currentlyGettingChanges = true;
  try {
    const addTablesString = onlyIncludeTables
      .map((table) => (table.includes(".") ? `${table}` : `${schema}.${table}`))
      .join(",");

    // options documented here: https://github.com/eulerto/wal2json
    const { rows } = await client.query<{ lsn: string; data: string }>(
      `select lsn, data from pg_catalog.pg_logical_slot_get_changes(
          $1, $2, $3,
          'include-transaction', 'false',
          'add-tables', $4
        )`,
      [
        slotName,
        null, // up to LSN
        null, // limit of changes to get
        addTablesString,
      ]
    );

    const changes = rows.map(toLsnData);
    changeHandler(changes);
  } catch (e) {
    invariant(
      e instanceof Error,
      "Expected error thrown from pg_logical_slot_get_changes to be an instance of Error, but got something else"
    );

    if ("code" in e && e.code === "42704") {
      console.warn("Replication slot went away?");
      await createSlot(client, slotName);
      console.warn(
        "Recreated slot; retrying getChanges (no further output implies success)"
      );

      await getChanges(
        client,
        slotName,
        schema,
        onlyIncludeTables,
        changeHandler
      );
    }

    throw e;
  } finally {
    currentlyGettingChanges = false;
  }
};

export const getKyselyChanges = async <
  Database extends MinimalKyselyDatabaseTemplate,
  IncludedTables extends keyof Database & string,
  PrimaryKeyMap extends
    | PrimaryKeyOverrideInput<Database, IncludedTables>
    | undefined
>(
  pg: Pool,
  db: Kysely<Database>,
  onlyIncludeTables: IncludedTables[],
  primaryKeyMap: PrimaryKeyMap,
  options?: WalConfigOptions
): Promise<KyselyChanges<Database, IncludedTables, PrimaryKeyMap>> => {
  // Destructure options and create constants
  const {
    slotId: userSlotId,
    pollInterval: userPollInterval,
    assumeSchema,
  } = options ?? {};

  const slotId = userSlotId || Math.random().toString().slice(2);
  const pollInterval = userPollInterval ?? POLL_INTERVAL_MS;
  const slotName = `app_slot_${slotId}`;
  const schema = assumeSchema ?? "public";

  // Create client, subject, and actually create the slot in the DB
  const client = await pg.connect();
  const subject = new Subject<
    SubjectChangeEvent<Database, IncludedTables, PrimaryKeyMap>
  >();
  await createSlot(client, slotName);

  const isUsingMultipleSchemas = onlyIncludeTables.some((table) =>
    table.includes(".")
  );

  // Start polling for changes
  const interval = setInterval(
    () =>
      getChanges(client, slotName, schema, onlyIncludeTables, (payloads) => {
        for (const payload of payloads) {
          for (const change of payload.data.change) {
            if (change.kind === "insert") {
              const pairs = change.columnnames.map((name, index) => [
                name,
                change.columnvalues[index],
              ]);

              const tableKey = (
                !isUsingMultipleSchemas
                  ? `${change.table}`
                  : `${schema}.${change.table}`
              ) as IncludedTables;

              const event = {
                table: tableKey,
                event: change.kind,
                row: Object.fromEntries(pairs),
              };

              subject.next(event);
            }

            if (change.kind === "update") {
              const pairs = change.columnnames.map((name, index) => [
                name,
                change.columnvalues[index],
              ]);

              const tableKey = (
                !isUsingMultipleSchemas
                  ? `${change.table}`
                  : `${schema}.${change.table}`
              ) as IncludedTables;

              const event = {
                table: tableKey,
                event: change.kind,
                row: Object.fromEntries(pairs),
              };

              subject.next(event);
            }

            if (change.kind === "delete") {
              const pairs = change.oldkeys.keynames.map((name, index) => [
                name,
                change.oldkeys.keyvalues[index],
              ]);

              const tableKey = (
                !isUsingMultipleSchemas
                  ? `${change.table}`
                  : `${schema}.${change.table}`
              ) as IncludedTables;

              const event = {
                table: tableKey,
                event: change.kind,
                identity: Object.fromEntries(pairs),
              };

              subject.next(event);
            }
          }
        }
      }),
    pollInterval
  );

  /**
   * Teardown function that should be called when you're done with the subject
   * It will drop the slot from the DB and release the client back to the pool
   * It will also complete the subject, completing any subscribers of it as well
   */
  const teardown = async () => {
    clearInterval(interval);
    await dropSlot(client, slotName);
    await client.release();
    subject.complete();
  };

  return { subject, db, teardown, primaryKeyMap };
};

export type GetObservableEventType<T> = T extends Subject<infer U> ? U : never;

export const listWalSlots = async (pg: Pool) => {
  const { rows } = await pg.query<{
    slot_name: string;
    plugin_name: string;
    database: string;
    temporary: boolean;
    active: boolean;
  }>(
    "select slot_name, plugin, slot_type, database, temporary, active from pg_replication_slots"
  );

  return rows;
};
