export * from "./domain";
export { classifyChangedFiles } from "./detectors/changed-files";
export {
  parseMutedEntries,
  diffMutedEntries,
  locateUnmutedTest,
  findUnmutedTests,
  type UnmuteDetectionResult,
} from "./detectors/unmutes";
export {
  classifyExplicitList,
  type ExplicitListResult,
  type UnresolvedSpec,
} from "./detectors/explicit-list";
export {
  dedupeTests,
  collapseYamlSuites,
  deduplicateYamlRunners,
  generateBatchCommand,
  buildCommands,
} from "./commands";
export { toBuildkitePipeline, uploadBuildkitePipeline } from "./runners/buildkite";
export { runLocally } from "./runners/local";
export { resolveMergeBaseTarget } from "./entrypoints/pr";
export {
  analyzeReports,
  classifyFailure,
  type FlakinessReport,
  type TestSummary,
  type BatchSummary,
  type FailureKind,
} from "./analyzer/analyze";
export { renderMarkdown, severity } from "./analyzer/render";

// generatePipeline backward-compat wrapper — used by existing tests.
import { buildCommands } from "./commands";
import { toBuildkitePipeline } from "./runners/buildkite";
import { DEFAULT_AGENT_CONFIG, DEFAULT_BATCHING_CONFIG } from "./domain";
import type { ClassifiedTest } from "./domain";

export function generatePipeline(tests: ClassifiedTest[]) {
  return toBuildkitePipeline(
    buildCommands(tests, DEFAULT_BATCHING_CONFIG),
    DEFAULT_AGENT_CONFIG
  );
}

if (import.meta.main) {
  // Legacy invocation path: anything still calling `bun .../index.ts`
  // runs the PR flow.
  const { run } = await import("./entrypoints/pr");
  run();
}
