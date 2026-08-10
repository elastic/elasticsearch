import { execSync } from "child_process";
import { readFileSync, writeFileSync } from "fs";
import { basename, join } from "path";
import { backportRun } from "backport";
import { parseRemovedBlocks, removeBlocks } from "./muted-tests.ts";

const MUTED_TESTS_FILE = "muted-tests.yml";

await backportRun({
  options: {
    autoFixConflicts({ files, directory }) {
      if (!files.some((f) => basename(f) === MUTED_TESTS_FILE)) {
        return false;
      }

      try {
        const diff = execSync(
          `git show CHERRY_PICK_HEAD -- ${MUTED_TESTS_FILE}`,
          { cwd: directory, encoding: "utf8" },
        );

        const removedBlocks = parseRemovedBlocks(diff);
        if (!removedBlocks || removedBlocks.length === 0) {
          return false;
        }

        // Reset to the target branch's clean version to eliminate conflict markers.
        execSync(`git checkout HEAD -- ${MUTED_TESTS_FILE}`, {
          cwd: directory,
        });

        const filePath = join(directory, MUTED_TESTS_FILE);
        const content = readFileSync(filePath, "utf8");
        writeFileSync(filePath, removeBlocks(content, removedBlocks));

        execSync(`git add ${MUTED_TESTS_FILE}`, { cwd: directory });

        // Verify no conflicts remain after our edit.
        const unmerged = execSync("git ls-files --unmerged", {
          cwd: directory,
          encoding: "utf8",
        });

        return unmerged.trim().length === 0;
      } catch {
        return false;
      }
    },
  },
  processArgs: process.argv.slice(2),
});
