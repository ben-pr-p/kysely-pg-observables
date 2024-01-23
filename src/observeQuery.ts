import { Observable } from "rxjs";
import {
  DeletePayload,
  UpdatePayload,
  InsertPayload,
  MinimalKyselyDatabaseTemplate,
  KyselyChanges,
} from "./types";

type TableMapHandler<
  Database extends MinimalKyselyDatabaseTemplate,
  Table extends keyof Database & string,
  PrimaryKeyMap,
  Result
> = {
  insert?: (
    row: InsertPayload<Database, Table>,
    lastResult: Result
  ) => boolean | Promise<boolean>;
  update?: (
    row: UpdatePayload<Database, Table>,
    lastResult: Result
  ) => boolean | Promise<boolean>;
  delete?: (
    row: DeletePayload<Database, Table, PrimaryKeyMap>,
    lastResult: Result
  ) => boolean | Promise<boolean>;
};

// type TableFunctionHandler<
//   Database extends MinimalKyselyDatabaseTemplate,
//   Table extends keyof Database & string,
//   PrimaryKeyMap
// > = (
//   change:
//     | { event: "insert"; row: InsertPayload<Database, Table> }
//     | { event: "update"; row: UpdatePayload<Database, Table> }
//     | {
//         event: "delete";
//         identity: DeletePayload<Database, Table, PrimaryKeyMap>;
//       }
// ) => boolean;

type HandlerMap<
  Database extends MinimalKyselyDatabaseTemplate,
  IncludedTables extends keyof Database & string,
  PrimaryKeyMap,
  Result
> = {
  [Table in IncludedTables]: TableMapHandler<
    Database,
    IncludedTables,
    PrimaryKeyMap,
    Result
  >;
};

export const observeQuery = <
  Database extends MinimalKyselyDatabaseTemplate,
  IncludedTables extends keyof Database & string,
  PrimaryKeyMap,
  Result
>(
  kyselyChanges: KyselyChanges<Database, IncludedTables, PrimaryKeyMap>,
  query: () => Promise<Result>,
  handlerMap: HandlerMap<Database, IncludedTables, PrimaryKeyMap, Result>
): Observable<Result> => {
  const { subject } = kyselyChanges;

  const observable = new Observable<Result>((subscriber) => {
    let runningQuery: Promise<void> | undefined = undefined;
    let queueNextQuery: (() => Promise<void>) | undefined;
    let lastResult: undefined | Result = undefined;

    const runQueryAndCallNextIfExists = () => {
      runningQuery = query()
        .then((result) => {
          // Query is no longer running
          // Could be queued again
          lastResult = result;
          runningQuery = undefined;

          // Send the first result
          subscriber.next(result);

          if (queueNextQuery) {
            const toRun = queueNextQuery;
            queueNextQuery = undefined;
            runningQuery = toRun();
          }
        })
        .catch((error) => {
          subscriber.error(error);
        });

      return runningQuery;
    };

    // Kick off a first invocation
    runQueryAndCallNextIfExists();

    // Start subscribing after a transaction starts to fetch the first result
    const subjectSubscription = subject.subscribe({
      next: async (change) => {
        const valueAtHandlerMapForTable = handlerMap[change.table];

        let shouldReRunQuery = false;

        if (typeof valueAtHandlerMapForTable === "object") {
          const maybeHandler = valueAtHandlerMapForTable[change.event];
          if (maybeHandler) {
            const handlerResult = await maybeHandler(
              ("row" in change ? change.row : change.identity) as any,
              lastResult as Result
            );
            shouldReRunQuery = handlerResult;
          }
        }

        if (shouldReRunQuery) {
          if (runningQuery === undefined && queueNextQuery === undefined) {
            // There is no query running and no query already queued
            // We should run the query immediately
            runningQuery = runQueryAndCallNextIfExists();
          } else if (
            runningQuery !== undefined &&
            queueNextQuery === undefined
          ) {
            // We need to queue one up by setting queueNextQuery to the run function
            // When the current query finishes, it will run queueNextQuery if defined
            queueNextQuery = runQueryAndCallNextIfExists;
          } else {
            // We don't need to do anything - someone else is already waiting to start
            // and they'll start with visibility of the current transaction we're reacting to
          }
        } else {
          // There is no handler for this change
          // Do nothing! We can't assume that the query needs to be re-run
        }
      },
      complete: () => {
        subscriber.complete();
      },
      error: (error) => {
        subscriber.error(error);
      },
    });

    return () => {
      subjectSubscription.unsubscribe();
    };
  });

  return observable;
};
