import { parse } from "yaml";

import type { FlakinessRef } from "../domain.ts";

interface RawMutedTest {
  class?: string;
  method?: string;
  methods?: string[];
  tasks?: string[];
}

interface RawMutedTestsFile {
  tests?: RawMutedTest[];
}

// A muted-tests.yml entry reduced to the fields we diff on. Not exported: the outside world only sees
// FlakinessRef now (the Java resolver owns turning a class/method into a project/sourceSet/kind, and any
// task filters into a filtered runnable-task set).
interface MutedEntry {
  className: string;
  method?: string;
  tasks?: string[];
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
    const tasks = normalizeTasks(t.tasks);

    if (!hasAnyMethod) {
      entries.push({ className: t.class, ...(tasks ? { tasks } : {}) });
      continue;
    }
    for (const m of methodsList) {
      entries.push({ className: t.class, method: m, ...(tasks ? { tasks } : {}) });
    }
    if (t.method !== undefined) {
      entries.push({ className: t.class, method: t.method, ...(tasks ? { tasks } : {}) });
    }
  }
  return entries;
}

function mutedEntryKey(e: Pick<MutedEntry, "className" | "method">): string {
  return `${e.className}|${e.method ?? ""}`;
}

function normalizeTasks(tasks?: string[]): string[] | undefined {
  if (!tasks || tasks.length === 0) return undefined;
  return [...new Set(tasks)].sort();
}

function normalizeMutedEntries(entries: MutedEntry[]): MutedEntry[] {
  const byKey = new Map<string, { className: string; method?: string; global: boolean; tasks: Set<string> }>();

  for (const entry of entries) {
    const key = mutedEntryKey(entry);
    const current = byKey.get(key) ?? {
      className: entry.className,
      ...(entry.method !== undefined ? { method: entry.method } : {}),
      global: false,
      tasks: new Set<string>(),
    };

    if (entry.tasks === undefined) {
      current.global = true;
      current.tasks.clear();
    } else if (current.global === false) {
      for (const task of entry.tasks) {
        current.tasks.add(task);
      }
    }

    byKey.set(key, current);
  }

  return [...byKey.values()].map((value) => ({
    className: value.className,
    ...(value.method !== undefined ? { method: value.method } : {}),
    ...(value.global ? {} : { tasks: [...value.tasks].sort() }),
  }));
}

export function diffMutedEntries(before: MutedEntry[], after: MutedEntry[]): FlakinessRef[] {
  const normalizedBefore = normalizeMutedEntries(before);
  const normalizedAfter = new Map(normalizeMutedEntries(after).map((entry) => [mutedEntryKey(entry), entry]));

  const refs: FlakinessRef[] = [];
  for (const entry of normalizedBefore) {
    const matching = normalizedAfter.get(mutedEntryKey(entry));
    if (!matching) {
      refs.push(unmuteRef(entry, entry.tasks ? { includeTasks: entry.tasks } : {}));
      continue;
    }

    if (entry.tasks === undefined) {
      if (matching.tasks !== undefined) {
        refs.push(unmuteRef(entry, { excludeTasks: matching.tasks }));
      }
      continue;
    }

    if (matching.tasks === undefined) {
      continue;
    }

    const matchingTasks = new Set(matching.tasks);
    const removedTasks = entry.tasks.filter((task) => matchingTasks.has(task) === false);
    if (removedTasks.length > 0) {
      refs.push(unmuteRef(entry, { includeTasks: removedTasks }));
    }
  }

  return refs;
}

function unmuteRef(
  entry: Pick<MutedEntry, "className" | "method">,
  filters: { includeTasks?: string[]; excludeTasks?: string[] }
): FlakinessRef {
  return {
    source: "unmute" as const,
    className: entry.className,
    ...(entry.method !== undefined ? { method: entry.method } : {}),
    ...(filters.includeTasks !== undefined ? { includeTasks: filters.includeTasks } : {}),
    ...(filters.excludeTasks !== undefined ? { excludeTasks: filters.excludeTasks } : {}),
  };
}

/**
 * Diff two `muted-tests.yml` texts and emit an `unmute` {@link FlakinessRef} for every mute that was
 * removed or narrowed. Resolution of these refs to a project/sourceSet/kind (including whether an unmuted
 * base class is abstract and should expand to subclasses, and filtering the runnable task set for
 * task-scoped unmutes) is now the Java resolver's job - this collector no longer needs the repo file list at
 * all, which is a nice simplification of the bootstrap step.
 */
export function findUnmutedRefs(oldYamlText: string, newYamlText: string): FlakinessRef[] {
  const before = parseMutedEntries(oldYamlText);
  const after = parseMutedEntries(newYamlText);
  return diffMutedEntries(before, after);
}
