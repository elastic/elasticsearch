import { readFileSync } from "fs";
import { spawnSync } from "child_process";
import { resolve } from "path";
import { describe, expect, test } from "vitest";
import { parse, parseAllDocuments } from "yaml";

const ROOT = resolve(import.meta.dirname, "../..");
const PIPELINE_FILE = ".buildkite/pipelines/pull-request-validate-prometheus-compliance.yml";
const pipeline = parse(readFileSync(resolve(ROOT, PIPELINE_FILE), "utf8"));
const catalog = parseAllDocuments(readFileSync(resolve(ROOT, "catalog-info.yaml"), "utf8")).map((doc) => doc.toJS());

// These are Buildkite expressions evaluated at upload, before any job runs.
const ONESHOT_CONDITION = 'build.source != "schedule"';
const NIGHTLY_CONDITION = 'build.source == "schedule"';

describe("shared promcheck YAML pipeline", () => {
  test.each([
    { args: [], message: "Unknown promcheck mode: <unset>" },
    { args: ["unknown"], message: "Unknown promcheck mode: unknown" },
    { args: ["oneshot", "unknown"], message: "Unknown oneshot action: unknown" },
  ])("rejects invalid invocation $args before starting work", ({ args, message }) => {
    const result = spawnSync("bash", [resolve(ROOT, ".buildkite/scripts/validate-prometheus-compliance.sh"), ...args], {
      encoding: "utf8",
    });
    expect(result.status).toBe(1);
    expect(result.stderr).toContain(message);
    expect(result.stdout).toBe("");
  });

  test("the existing pipeline retains its identity and gains the nightly schedule", () => {
    const entries = catalog.filter((entry) => entry.spec?.implementation?.spec?.pipeline_file === PIPELINE_FILE);
    expect(entries).toHaveLength(1);
    expect(entries[0].metadata.name).toBe("buildkite-pipeline-elasticsearch-pull-request-prom-compliance");
    const config = entries[0].spec.implementation.spec;
    expect(config.schedules["elastic / prometheus / nightly"]).toMatchObject({ branch: "main", cronline: "@daily" });
    expect(config.env.ELASTIC_PR_COMMENTS_ENABLED).toBe("true");
    expect(config.teams).toEqual({ "elasticsearch-team": {}, everyone: { access_level: "BUILD_AND_READ" } });
    expect(catalog.some((entry) => entry.metadata?.name === "buildkite-pipeline-elastic-prometheus-nightly")).toBe(false);
  });

  test("build source alone selects nightly or the PR-only steps", () => {
    expect(pipeline.steps.filter((step) => step.if === ONESHOT_CONDITION).map((step) => step.key)).toEqual([
      "notify", "validate-prometheus-compliance",
    ]);
    expect(pipeline.steps.filter((step) => step.if === NIGHTLY_CONDITION).map((step) => step.key)).toEqual(["prometheus-nightly"]);
  });

  test("both modes and the notification use the single script", () => {
    const script = ".buildkite/scripts/validate-prometheus-compliance.sh";
    expect(pipeline.steps.map((step) => step.command)).toEqual([
      `${script} oneshot notify`, `${script} oneshot`, `${script} nightly`,
    ]);
  });

  test("anchors share the runner settings while retaining PR-only retries", () => {
    const pr = pipeline.steps.find((step) => step.key === "validate-prometheus-compliance");
    const nightly = pipeline.steps.find((step) => step.key === "prometheus-nightly");
    expect(nightly.agents).toEqual(pr.agents);
    expect(nightly.timeout_in_minutes).toBe(pr.timeout_in_minutes);
    expect(pr.retry.automatic).toHaveLength(2);
    expect(nightly.retry).toBeUndefined();
  });

  test("Overview configuration is scoped to the experimental nightly step", () => {
    expect(pipeline.env.PROMCHECK_RESULTS_SECRET).toBeUndefined();
    expect(pipeline.env.PROMCHECK_RESULTS_NAMESPACE).toBeUndefined();
    const nightly = pipeline.steps.find((step) => step.key === "prometheus-nightly");
    expect(nightly.env.PROMCHECK_RESULTS_NAMESPACE).toBe("experimental");
    expect(nightly.env.PROMCHECK_RESULTS_URL).toBe("https://overview-elastic-cloud-com.es.us-east-1.aws.found.io");
  });
});
