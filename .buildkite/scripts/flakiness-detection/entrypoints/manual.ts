import { writeFileSync } from "fs";
import { resolve } from "path";

import { explicitRefs } from "../detectors/explicit-list.ts";
import { uploadResolvePipeline } from "../runners/buildkite.ts";
import { DEFAULT_AGENT_CONFIG, type FlakinessRefsFile } from "../domain.ts";

const REFS_FILE = "flakiness-refs.json";
const PROJECT_ROOT = resolve(`${import.meta.dirname}/../../../..`);

export function run(): void {
  const rawClasses = process.env.FLAKINESS_CLASSES;
  if (!rawClasses || rawClasses.trim() === "") {
    console.error("FLAKINESS_CLASSES environment variable is required (newline- or comma-separated)");
    process.exit(2);
  }
  const specs = rawClasses.split(/[\n,]/);
  const refs = explicitRefs(specs);
  if (refs.length === 0) {
    console.error("FLAKINESS_CLASSES contained no non-blank specs");
    process.exit(2);
  }

  // No mergeBase for a manual run; the resolver only uses it for logging. Iteration counts are now owned by
  // the Java scan task (baked into the plan's batch commands), so this bootstrap only writes the refs.
  const refsFile: FlakinessRefsFile = { mergeBase: "", refs };
  writeFileSync(resolve(PROJECT_ROOT, REFS_FILE), JSON.stringify(refsFile, null, 2));
  console.log(`Wrote ${refs.length} explicit refs to ${REFS_FILE}`);

  uploadResolvePipeline(DEFAULT_AGENT_CONFIG);
}

if (import.meta.main) run();
