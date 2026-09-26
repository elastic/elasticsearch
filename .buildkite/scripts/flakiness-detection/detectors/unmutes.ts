import { parse } from "yaml";

import type {
  ClassifiedTest,
  TestRef,
} from "../domain.ts";
import { locateTest } from "./locator.ts";

interface RawMutedTest {
  class?: string;
  method?: string;
  methods?: string[];
}

interface RawMutedTestsFile {
  tests?: RawMutedTest[];
}

export function parseMutedEntries(yamlText: string): TestRef[] {
  if (yamlText.trim() === "") return [];
  const parsed = parse(yamlText) as RawMutedTestsFile | null;
  const rawTests = parsed?.tests ?? [];

  const entries: TestRef[] = [];
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

function mutedEntryKey(e: TestRef): string {
  return `${e.className}|${e.method ?? ""}`;
}

export function diffMutedEntries(before: TestRef[], after: TestRef[]): TestRef[] {
  const afterKeys = new Set(after.map(mutedEntryKey));
  return before.filter((e) => afterKeys.has(mutedEntryKey(e)) === false);
}

export interface UnmuteDetectionResult {
  located: ClassifiedTest[];
  unlocated: TestRef[];
}

export function findUnmutedTests(
  oldYamlText: string,
  newYamlText: string,
  repoFiles: string[]
): UnmuteDetectionResult {
  const before = parseMutedEntries(oldYamlText);
  const after = parseMutedEntries(newYamlText);
  const unmuted = diffMutedEntries(before, after);

  const located: ClassifiedTest[] = [];
  const unlocated: TestRef[] = [];
  for (const entry of unmuted) {
    const test = locateTest(entry, repoFiles);
    if (test === null) {
      unlocated.push(entry);
    } else {
      located.push(test);
    }
  }
  return { located, unlocated };
}
