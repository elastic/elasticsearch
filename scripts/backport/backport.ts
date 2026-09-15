import { execSync } from "child_process";
import { readFileSync, writeFileSync } from "fs";
import { basename, join } from "path";
import { backportRun } from "backport";
import { diffRemovedEntries, removeEntries } from "./muted-tests.ts";

const MUTED_TESTS_FILE = "muted-tests.yml";

await backportRun({
  options: {
    autoFixConflicts({ files, directory, logger }) {
      if (!files.some((f) => basename(f) === MUTED_TESTS_FILE)) {
        return false;
      }

      try {
        const show = (rev: string) =>
          execSync(`git show ${rev}:${MUTED_TESTS_FILE}`, {
            cwd: directory,
            encoding: "utf8",
          });

        // Compare the file as it looked either side of the commit being
        // cherry-picked, rather than parsing its diff.
        const removed = diffRemovedEntries(
          show("CHERRY_PICK_HEAD^"),
          show("CHERRY_PICK_HEAD"),
        );
        if (!removed || removed.length === 0) {
          logger.info(
            `${MUTED_TESTS_FILE}: not a pure unmuting, skipping auto-fix`,
          );
          return false;
        }

        // Reset to the target branch's clean version to eliminate conflict markers.
        execSync(`git checkout HEAD -- ${MUTED_TESTS_FILE}`, {
          cwd: directory,
        });

        const filePath = join(directory, MUTED_TESTS_FILE);
        const content = readFileSync(filePath, "utf8");
        writeFileSync(filePath, removeEntries(content, removed));

        logger.info(
          `${MUTED_TESTS_FILE}: unmuted ${removed
            .map((e) => `${e.class}${e.method ? `#${e.method}` : ""}`)
            .join(", ")}`,
        );

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
