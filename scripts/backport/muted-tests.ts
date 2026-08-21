import { isMap, isScalar, isSeq, parseDocument } from "yaml";

export interface MutedTestEntry {
  class: string;
  /** Absent means the entire class is muted. */
  method?: string;
  issue?: string;
}

/**
 * Identity key for diffing: class+method only. Issue is intentionally omitted
 * so that a changed issue link across branches doesn't look like an add+remove.
 * Issue equality is validated separately during removal.
 */
export function entryKey(entry: MutedTestEntry): string {
  return JSON.stringify([entry.class, entry.method ?? null]);
}

/**
 * Parses the `tests:` sequence of muted-tests.yml into entries. Entries that
 * use `methods: [...]` are expanded into one `MutedTestEntry` per method.
 */
export function parseMutedTests(content: string): MutedTestEntry[] {
  const doc = parseDocument(content);
  if (doc.errors.length > 0) {
    throw new Error(`invalid YAML: ${doc.errors[0]!.message}`);
  }

  const tests = doc.get("tests");
  // An empty `tests:` key parses as null — a valid, empty mute list.
  if (tests === null || tests === undefined) return [];
  if (!isSeq(tests)) throw new Error("`tests` is not a sequence");

  const result: MutedTestEntry[] = [];
  for (const item of tests.items) {
    if (!isMap(item)) throw new Error("mute entry is not a mapping");
    const entry = item.toJSON() as Record<string, unknown>;
    if (typeof entry.class !== "string") {
      throw new Error("mute entry has no `class`");
    }
    const cls = entry.class;
    const issue = typeof entry.issue === "string" ? entry.issue : undefined;

    if (Array.isArray(entry.methods)) {
      for (const m of entry.methods) {
        if (typeof m !== "string") {
          throw new Error(`methods entry for ${cls} is not a string`);
        }
        result.push({ class: cls, method: m, issue });
      }
    } else {
      result.push({
        class: cls,
        method: typeof entry.method === "string" ? entry.method : undefined,
        issue,
      });
    }
  }
  return result;
}

/**
 * Compares the pre- and post-commit versions of muted-tests.yml and returns
 * the entries the commit removed. Returns null if the commit is not a pure
 * unmuting — i.e. it also adds entries or otherwise modifies the file.
 */
export function diffRemovedEntries(
  before: string,
  after: string,
): MutedTestEntry[] | null {
  const beforeEntries = parseMutedTests(before);
  const afterEntries = parseMutedTests(after);

  const afterKeys = new Set(afterEntries.map(entryKey));
  const beforeKeys = new Set(beforeEntries.map(entryKey));

  // Any added entry means this is a muting (or mixed) commit, not an unmuting.
  if (afterEntries.some((e) => !beforeKeys.has(entryKey(e)))) return null;

  return beforeEntries.filter((e) => !afterKeys.has(entryKey(e)));
}

/** Maps a character offset to its 0-based line index. */
function lineIndexOf(lineStarts: number[], offset: number): number {
  for (let i = lineStarts.length - 1; i >= 0; i--) {
    if (offset >= lineStarts[i]!) return i;
  }
  return 0;
}

/** Throws if the issue link recorded in the cherry-pick differs from the target branch. */
function checkIssue(
  wanted: MutedTestEntry,
  targetIssue: string | undefined,
  label: string,
): void {
  if (
    wanted.issue !== undefined &&
    targetIssue !== undefined &&
    wanted.issue !== targetIssue
  ) {
    throw new Error(
      `issue mismatch for ${label}: cherry-pick has ${wanted.issue}, target branch has ${targetIssue}`,
    );
  }
}

/**
 * Removes the given entries from muted-tests.yml by deleting exactly the
 * source lines each entry occupies. The YAML parser locates the entries; the
 * edit itself is a line splice, so no other line in the file is reformatted.
 *
 * For `methods:` entries, if only a subset of methods are removed the surviving
 * methods are kept; if all methods are removed the whole entry is deleted.
 *
 * Entries that are already absent are silently skipped.
 *
 * Throws if an issue link differs between the cherry-pick and target branch,
 * so the caller can fall back to manual resolution.
 */
export function removeEntries(
  content: string,
  entries: MutedTestEntry[],
): string {
  if (entries.length === 0) return content;

  const doc = parseDocument(content);
  if (doc.errors.length > 0) {
    throw new Error(`invalid YAML: ${doc.errors[0]!.message}`);
  }

  const tests = doc.get("tests");
  if (!isSeq(tests)) {
    if (tests === null || tests === undefined) return content;
    throw new Error("`tests` is not a sequence");
  }

  const eol = content.includes("\r\n") ? "\r\n" : "\n";
  const lines = content.split(eol);

  // Character offset at which each line begins.
  const lineStarts: number[] = [];
  let offset = 0;
  for (const line of lines) {
    lineStarts.push(offset);
    offset += line.length + eol.length;
  }

  /** Add lines [startLine, endLine] to the removal set, verifying the layout. */
  function markEntryLines(
    item: { range?: [number, number, number] | null },
  ): void {
    if (!item.range) throw new Error("mute entry has no source range");
    const [start, valueEnd] = item.range;
    const startLine = lineIndexOf(lineStarts, start);
    const endLine = lineIndexOf(lineStarts, Math.max(valueEnd - 1, start));

    // The map's range begins after the `- ` marker. Deleting whole lines is
    // only safe if nothing but the marker precedes it on that line.
    const prefix = lines[startLine]!.slice(0, start - lineStarts[startLine]!);
    if (!/^\s*-\s+$/.test(prefix)) {
      throw new Error(`unexpected entry layout at line ${startLine + 1}`);
    }

    for (let i = startLine; i <= endLine; i++) linesToRemove.add(i);
  }

  // Map from identity key to the full entry so we can validate the issue link.
  const targetEntries = new Map(entries.map((e) => [entryKey(e), e]));
  const linesToRemove = new Set<number>();

  for (const item of tests.items) {
    if (!isMap(item)) continue;
    const json = item.toJSON() as Record<string, unknown>;
    if (typeof json.class !== "string") continue;

    const cls = json.class;
    const itemIssue = typeof json.issue === "string" ? json.issue : undefined;

    if (Array.isArray(json.methods)) {
      // methods: entry — check which methods are in the removal set.
      const methodsToRemove = (json.methods as unknown[]).filter(
        (m): m is string =>
          typeof m === "string" &&
          targetEntries.has(entryKey({ class: cls, method: m })),
      );
      if (methodsToRemove.length === 0) continue;

      // Validate issue for all methods being removed (they share the same issue).
      const representative = targetEntries.get(
        entryKey({ class: cls, method: methodsToRemove[0]! }),
      )!;
      checkIssue(representative, itemIssue, `${cls} (methods block)`);

      if (methodsToRemove.length === json.methods.length) {
        // All methods removed — delete the entire entry.
        markEntryLines(item);
      } else {
        // Partial removal — splice out individual method items.
        const methodsToRemoveSet = new Set(methodsToRemove);
        const methodsSeq = item.get("methods", true);
        if (!isSeq(methodsSeq)) continue;

        for (const methodItem of methodsSeq.items) {
          if (!isScalar(methodItem)) continue;
          if (!methodsToRemoveSet.has(String(methodItem.value))) continue;
          if (!methodItem.range) continue;

          const [mStart, , mEnd] = methodItem.range;
          const mStartLine = lineIndexOf(lineStarts, mStart);
          const mEndLine = lineIndexOf(lineStarts, Math.max(mEnd - 1, mStart));
          for (let i = mStartLine; i <= mEndLine; i++) linesToRemove.add(i);
        }
      }
    } else {
      // method: or whole-class entry.
      const key = entryKey({
        class: cls,
        method: typeof json.method === "string" ? json.method : undefined,
      });
      const wanted = targetEntries.get(key);
      if (!wanted) continue;

      checkIssue(
        wanted,
        itemIssue,
        `${cls}${wanted.method ? `#${wanted.method}` : ""}`,
      );
      markEntryLines(item);
    }
  }

  return lines.filter((_, i) => !linesToRemove.has(i)).join(eol);
}
