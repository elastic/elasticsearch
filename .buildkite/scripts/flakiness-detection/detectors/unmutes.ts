import { parse } from "yaml";
import { execSync } from "child_process";
import { readFileSync } from "fs";
import { resolve } from "path";

import {
  ClassifiedTest,
  MutedEntry,
  SOURCE_SET_PATTERNS,
  toFqcn,
  toGradleProject,
} from "../domain";

interface RawMutedTest {
  class?: string;
  method?: string;
  methods?: string[];
}

interface RawMutedTestsFile {
  tests?: RawMutedTest[];
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

const YAML_METHOD_REGEX = /^test \{yaml=.+\}$/;

export function locateUnmutedTest(entry: MutedEntry, repoFiles: string[]): ClassifiedTest | null {
  const pathSuffix = entry.className.replace(/\./g, "/") + ".java";
  const candidate = repoFiles.find((f) => f === pathSuffix || f.endsWith("/" + pathSuffix));
  if (candidate === undefined) return null;

  for (const pattern of SOURCE_SET_PATTERNS) {
    const match = candidate.match(pattern.regex);
    if (match === null) continue;

    const gradleProject = toGradleProject(match[1]);
    const kind = pattern.kind;

    switch (kind) {
      case "test":
      case "internalClusterTest":
      case "javaRestTest":
        return {
          gradleProject,
          kind,
          sourceSet: pattern.sourceSet,
          fqcn: toFqcn(match[2]),
        };
      case "yamlRestTestRunner":
        // A parameterized yaml test case is identified by its full descriptor
        // "test {yaml=<path>/<test name>}". We target it exactly via
        // `--tests "<FQCN>.test {yaml=...}"` rather than `tests.rest.suite`,
        // which only accepts file/directory paths and cannot address an
        // individual case.
        if (entry.method !== undefined && YAML_METHOD_REGEX.test(entry.method)) {
          return {
            gradleProject,
            kind: "yamlRestTestCase",
            sourceSet: "yamlRestTest",
            fqcn: entry.className,
            yamlTest: entry.method,
          };
        }
        return {
          gradleProject,
          kind: "yamlRestTestRunner",
          sourceSet: "yamlRestTest",
        };
      case "yamlRestTestSuite":
        // Unreachable: muted-tests entries reference a Java class, but
        // yamlRestTestSuite only matches `.yml` resources. Fail loudly so
        // any future change to SOURCE_SET_PATTERNS that breaks this
        // invariant surfaces here rather than later as a malformed
        // ClassifiedTest in generateBatchCommand.
        throw new Error(`yamlRestTestSuite pattern unexpectedly matched Java file ${candidate}`);
      default:
        return assertNever(kind);
    }
  }
  return null;
}

function assertNever(x: never): never {
  throw new Error(`Unhandled SOURCE_SET_PATTERN kind: ${x as string}`);
}

export interface UnmuteDetectionResult {
  located: ClassifiedTest[];
  unlocated: MutedEntry[];
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
  const unlocated: MutedEntry[] = [];
  for (const entry of unmuted) {
    const test = locateUnmutedTest(entry, repoFiles);
    if (test === null) {
      unlocated.push(entry);
    } else {
      located.push(test);
    }
  }
  return { located, unlocated };
}

export function detectUnmutedTests(mergeBase: string, projectRoot: string): UnmuteDetectionResult {
  let oldYaml = "";
  try {
    oldYaml = execSync(`git show ${mergeBase}:muted-tests.yml`, {
      cwd: projectRoot,
      stdio: ["ignore", "pipe", "ignore"],
    }).toString();
  } catch {
    // File didn't exist at merge base; treat as empty.
  }

  let newYaml = "";
  try {
    newYaml = readFileSync(resolve(projectRoot, "muted-tests.yml"), "utf8");
  } catch {
    // File was deleted in the PR; treat as empty.
  }

  const repoFilesOutput = execSync("git ls-files", {
    cwd: projectRoot,
    maxBuffer: 256 * 1024 * 1024,
  }).toString();
  const repoFiles = repoFilesOutput
    .split("\n")
    .map((f) => f.trim())
    .filter((f) => f !== "");

  return findUnmutedTests(oldYaml, newYaml, repoFiles);
}
