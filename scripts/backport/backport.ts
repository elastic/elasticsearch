import { backportRun } from "backport";

await backportRun({
  options: {
    autoFixConflicts: (opts) => {
      console.log(opts);
      return false;
    },
  },
  processArgs: process.argv.slice(2),
});
