import { execSync } from "child_process";
import { readFileSync, writeFileSync } from "fs";
import { basename, join } from "path";
import { backportRun } from "backport";

const MUTED_TESTS_FILE = "muted-tests.yml";

// Returns removed entry blocks (each a list of raw lines) from the cherry-pick
// commit's diff, or null if the commit also adds lines to the file.
function parseRemovedBlocks(diff: string): string[][] | null {
  const blocks: string[][] = [];
  let currentBlock: string[] | null = null;
  let inHunk = false;

  for (const line of diff.split("\n")) {
    if (
      line.startsWith("diff ") ||
      line.startsWith("index ") ||
      line.startsWith("--- ") ||
      line.startsWith("+++ ")
    ) {
      currentBlock = null;
      continue;
    }
    if (line.startsWith("@@")) {
      inHunk = true;
      currentBlock = null;
      continue;
    }
    if (!inHunk) continue;

    if (line.startsWith("+")) {
      // The commit adds lines to muted-tests.yml — not a pure unmuting; bail.
      return null;
    }

    if (line.startsWith("-")) {
      const content = line.slice(1);
      if (content.startsWith("- class:")) {
        currentBlock = [content];
        blocks.push(currentBlock);
      } else if (currentBlock !== null) {
        currentBlock.push(content);
      }
    } else {
      // Context line — close the current entry block.
      currentBlock = null;
    }
  }

  return blocks;
}

// Removes exact line sequences from content without touching anything else.
// Preserves the original line-ending style.
function removeBlocks(content: string, blocks: string[][]): string {
  const eol = content.includes("\r\n") ? "\r\n" : "\n";
  const lines = content.split(eol);
  const toRemove = new Set<number>();

  for (const block of blocks) {
    for (let i = 0; i <= lines.length - block.length; i++) {
      if (block.every((blockLine, j) => lines[i + j] === blockLine)) {
        for (let j = 0; j < block.length; j++) toRemove.add(i + j);
        break;
      }
    }
  }

  return lines.filter((_, i) => !toRemove.has(i)).join(eol);
}

await backportRun({
  options: {
    autoFixConflicts({ files, directory }) {
      console.log(files);
      if (!files.some((f) => basename(f) === MUTED_TESTS_FILE)) return false;

      try {
        const diff = execSync(
          `git show CHERRY_PICK_HEAD -- ${MUTED_TESTS_FILE}`,
          { cwd: directory, encoding: "utf8" },
        );

        const removedBlocks = parseRemovedBlocks(diff);
        if (!removedBlocks || removedBlocks.length === 0) return false;

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
