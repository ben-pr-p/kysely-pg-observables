import { Kysely, Selectable } from "kysely";
import { Subject } from "rxjs";

export type MinimalKyselyDatabaseTemplate = {
  [table: string]: {
    [column: string]: unknown;
  };
};

export type TableWithIdTemplate = { id: unknown };

type DatabaseWithOnlyPrimaryKeys<Table, NonStandardKeys> =
  // If the tables keys are undefined and table has an id
  // then return the id
  // Otherwise, there is no known primary key
  NonStandardKeys extends undefined
    ? Table extends TableWithIdTemplate
      ? Pick<Table, "id">
      : {}
    : // Otherwise, if the tables keys are specified and in the table, use them
    NonStandardKeys extends readonly (keyof Table & string)[]
    ? Pick<Table, NonStandardKeys[number]>
    : never;

export type PrimaryKeyOverrideInput<
  Database,
  IncludedTables extends keyof Database & string
> = {
  readonly [table in IncludedTables]?: readonly (keyof Database[table] &
    string)[];
};

export type InsertPayload<
  Database extends MinimalKyselyDatabaseTemplate,
  Table extends keyof Database & string
> = Selectable<Database[Table]>;

export type UpdatePayload<
  Database extends MinimalKyselyDatabaseTemplate,
  Table extends keyof Database & string
> = Selectable<Database[Table]>;

export type DeletePayload<
  Database extends MinimalKyselyDatabaseTemplate,
  Table extends keyof Database & string,
  PrimaryKeyMap
> = PrimaryKeyMap extends PrimaryKeyOverrideInput<Database, Table>
  ? Selectable<
      DatabaseWithOnlyPrimaryKeys<Database[Table], PrimaryKeyMap[Table]>
    >
  : Selectable<Pick<Database[Table], "id">>;

export type SubjectChangeEvent<
  Database extends MinimalKyselyDatabaseTemplate,
  IncludedTables extends keyof Database & string,
  PrimaryKeyMap
> =
  | {
      table: IncludedTables;
      event: "insert";
      row: InsertPayload<Database, IncludedTables>;
    }
  | {
      table: IncludedTables;
      event: "update";
      row: UpdatePayload<Database, IncludedTables>;
    }
  | {
      table: IncludedTables;
      event: "delete";
      identity: DeletePayload<Database, IncludedTables, PrimaryKeyMap>;
    };

export type KyselyChanges<
  Database extends MinimalKyselyDatabaseTemplate,
  IncludedTables extends keyof Database & string,
  PrimaryKeyMap
> = {
  subject: Subject<SubjectChangeEvent<Database, IncludedTables, PrimaryKeyMap>>;
  db: Kysely<Database>;
  teardown: () => Promise<void>;
  primaryKeyMap: PrimaryKeyMap;
};
