import { isMap, isSeq, parseDocument } from "yaml";

/**
 * A single entry in muted-tests.yml. `issue` is incidental metadata and is
 * deliberately excluded from an entry's identity — the same mute is often
 * filed against different issue links across branches.
 */
export interface MutedTestEntry {
  class: string;
  method?: string;
}

/** Identity of a mute: the class/method pair it silences. */
export function entryKey(entry: MutedTestEntry): string {
  return JSON.stringify([entry.class, entry.method ?? null]);
}

/**
 * Parses the `tests:` sequence of muted-tests.yml into entries.
 * Throws if the document is not in the expected shape.
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

  return tests.items.map((item) => {
    if (!isMap(item)) throw new Error("mute entry is not a mapping");
    const entry = item.toJSON() as Record<string, unknown>;
    if (typeof entry.class !== "string") {
      throw new Error("mute entry has no `class`");
    }
    return {
      class: entry.class,
      method: typeof entry.method === "string" ? entry.method : undefined,
    };
  });
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

/**
 * Removes the given entries from muted-tests.yml by deleting exactly the
 * source lines each entry occupies. The YAML parser locates the entries; the
 * edit itself is a line splice, so no other line in the file is reformatted.
 *
 * Entries that are already absent are skipped — the desired end state (the
 * test is not muted) already holds.
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

  const targetKeys = new Set(entries.map(entryKey));
  const linesToRemove = new Set<number>();

  for (const item of tests.items) {
    if (!isMap(item)) continue;
    const json = item.toJSON() as Record<string, unknown>;
    if (typeof json.class !== "string") continue;

    const key = entryKey({
      class: json.class,
      method: typeof json.method === "string" ? json.method : undefined,
    });
    if (!targetKeys.has(key)) continue;

    if (!item.range) throw new Error("mute entry has no source range");
    const [start, valueEnd] = item.range;
    const startLine = lineIndexOf(lineStarts, start);
    const endLine = lineIndexOf(lineStarts, Math.max(valueEnd - 1, start));

    // The map's range begins after the `- ` marker. Deleting whole lines is
    // only safe if nothing but the marker precedes it on that line, which
    // rules out flow-style sequences such as `tests: [{class: ...}]`.
    const prefix = lines[startLine]!.slice(0, start - lineStarts[startLine]!);
    if (/^\s*-\s+$/.test(prefix) === false) {
      throw new Error(`unexpected entry layout at line ${startLine + 1}`);
    }

    for (let i = startLine; i <= endLine; i++) linesToRemove.add(i);
  }

  return lines.filter((_, i) => !linesToRemove.has(i)).join(eol);
}
