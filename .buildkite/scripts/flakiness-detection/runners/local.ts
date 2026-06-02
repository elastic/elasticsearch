import { execSync } from "child_process";
import { resolve } from "path";

import { RunnableCommand } from "../domain.ts";

const PROJECT_ROOT = resolve(`${import.meta.dirname}/../../../..`);

/**
 * Run each RunnableCommand sequentially via execSync, streaming output to the
 * parent stdio. On failure, record the exit code but continue so the developer
 * sees every batch's result, not just the first failure. Returns the maximum
 * exit code seen.
 */
export function runLocally(
  commands: RunnableCommand[],
  cwd: string = PROJECT_ROOT
): number {
  let worst = 0;
  for (const c of commands) {
    console.log(`\n>>> [${c.key}] ${c.label}`);
    console.log(`>>> ${c.command}`);
    try {
      execSync(c.command, { cwd, stdio: "inherit" });
    } catch (err) {
      const code = (err as { status?: number }).status ?? 1;
      console.error(`>>> [${c.key}] exited with code ${code}`);
      if (code > worst) worst = code;
    }
  }
  return worst;
}
