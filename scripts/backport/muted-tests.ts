/**
 * Returns the muted-test entry blocks that were removed by the cherry-pick
 * commit, as lists of raw file lines. Returns null if the commit also adds
 * lines to the file (i.e. it is not a pure unmuting).
 */
export function parseRemovedBlocks(diff: string): string[][] | null {
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

/**
 * Removes exact line sequences from content without touching anything else.
 * Preserves the original line-ending style.
 */
export function removeBlocks(content: string, blocks: string[][]): string {
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
