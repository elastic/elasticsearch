import { parse } from "yaml";

import type { FlakinessRef } from "../domain.ts";

interface RawMutedTest {
  class?: string;
  method?: string;
  methods?: string[];
}

interface RawMutedTestsFile {
  tests?: RawMutedTest[];
}

// A muted-tests.yml entry reduced to the fields we diff on. Not exported: the outside world only sees
// FlakinessRef now (the Java resolver owns turning a class/method into a project/sourceSet/kind).
interface MutedEntry {
  className: string;
  method?: string;
}

export function parseMutedEntries(yamlText: string): MutedEntry[] {
  if (yamlText.trim() === "") return [];
  const parsed = parse(yamlText) as RawMutedTestsFile | null;
  const rawTests = parsed?.tests ?? [];

  const entries: MutedEntry[] = [];
  for (const t of rawTests) {
    if (!t.class) continue;
    const methodsList = t.methods ?? [];
    const hasAnyMethod = methodsList.length > 0 || t.method !== undefined;

    if (!hasAnyMethod) {
      entries.push({ className: t.class });
      continue;
    }
    for (const m of methodsList) {
      entries.push({ className: t.class, method: m });
    }
    if (t.method !== undefined) {
      entries.push({ className: t.class, method: t.method });
    }
  }
  return entries;
}

function mutedEntryKey(e: MutedEntry): string {
  return `${e.className}|${e.method ?? ""}`;
}

export function diffMutedEntries(before: MutedEntry[], after: MutedEntry[]): MutedEntry[] {
  const afterKeys = new Set(after.map(mutedEntryKey));
  return before.filter((e) => afterKeys.has(mutedEntryKey(e)) === false);
}

/**
 * Diff two `muted-tests.yml` texts and emit an `unmute` {@link FlakinessRef} for every entry that was
 * removed. Resolution of these refs to a project/sourceSet/kind (including whether an unmuted base class
 * is abstract and should expand to subclasses) is now the Java resolver's job - this gatherer no longer
 * needs the repo file list at all, which is a nice simplification of the bootstrap step.
 */
export function findUnmutedRefs(oldYamlText: string, newYamlText: string): FlakinessRef[] {
  const before = parseMutedEntries(oldYamlText);
  const after = parseMutedEntries(newYamlText);
  return diffMutedEntries(before, after).map((e) => ({
    source: "unmute" as const,
    className: e.className,
    ...(e.method !== undefined ? { method: e.method } : {}),
  }));
}
