import { execFileSync, spawnSync } from "child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join, resolve } from "path";
import { afterEach, beforeEach, describe, expect, test } from "vitest";

const SCRIPT = resolve(`${import.meta.dirname}/validate-prometheus-compliance.sh`);
const CORPUS = "grafanalabs-top100";
const VERSION = "0.9.5+prometheus-3.13.1";

type Status = "match" | "mismatch" | "error" | "error4xx" | "error5xx" | "flaky" | "filtered";
type Case = [id: string, status: Status, expr: string];

/** JSONL records matching promcheck -o json, with the full expression. */
function promcheckLog(cases: Case[]): string {
  const count = (status: Status) => cases.filter(([, value]) => value === status).length;
  const rows = cases.map(([name, status, query]) => ({
    type: "query", name, query,
    status: status === "flaky" || status === "filtered" ? "skipped" : status.startsWith("error") ? "error" : status,
    ...(status === "error4xx" ? { es_status: 400 } : status === "error5xx" ? { es_status: 503 } : {}),
    ...(status === "flaky" || status === "filtered" ? { skip_reason: status === "flaky" ? "flaky" : "response_filtered" } : {}),
  }));
  return [
    { type: "build_info", version: VERSION },
    ...rows,
    { type: "summary", total: cases.length, inspected: cases.length,
      match: count("match"), mismatch: count("mismatch"),
      es_4xx: count("error4xx"), es_5xx: count("error5xx"), error: count("error"),
      request_filtered: 0, response_filtered: count("filtered"), flaky: count("flaky") },
  ].map((row) => JSON.stringify(row)).join("\n") + "\n";
}

describe("validate-prometheus-compliance.sh", () => {
  let dir: string;
  let repo: string;

  /** Runs git in `cwd` with a fixed identity, so commits work on any machine. */
  function git(cwd: string, ...args: string[]): string {
    return execFileSync("git", ["-c", "user.name=t", "-c", "user.email=t@t", "-c", "init.defaultBranch=main", ...args], {
      cwd,
      stdio: "pipe",
    })
      .toString()
      .trim();
  }

  function commit(cwd: string, file: string): string {
    writeFileSync(join(cwd, file), file);
    git(cwd, "add", file);
    git(cwd, "commit", "-q", "-m", file);
    return git(cwd, "rev-parse", "HEAD");
  }

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "prometheus-compliance-"));
    const bin = join(dir, "bin");
    mkdirSync(bin);

    // A target branch on an origin, and a PR branch off it: the script fetches origin/main and builds its merge base.
    git(dir, "init", "-q", "--bare", "origin.git");
    repo = join(dir, "repo");
    git(dir, "init", "-q", "repo");
    git(repo, "remote", "add", "origin", join(dir, "origin.git"));
    commit(repo, "base");
    git(repo, "push", "-q", "origin", "main");
    git(repo, "checkout", "-q", "-b", "pr");
    commit(repo, "pr-change");

    // Release assets: the binary for both arches (the script downloads its own) and the corpus bundle. The binary
    // is a mock: it prints the canned log of the run the --elasticsearch checkout stands for (the PR checkout is
    // the test run, anything else the control), records the commit it was built from, and exits with its code.
    const assets = join(dir, "assets");
    mkdirSync(assets);
    const fakeBin =
      `#!/bin/bash
while (($#)); do [[ $1 == --elasticsearch ]] && { ws=$2; break; }; shift; done
[[ $(cd "$ws" && pwd -P) == $(pwd -P) ]] && run=test || run=control
git -C "$ws" rev-parse HEAD > "${dir}/$run.head"
printf '%s\\n' "$*" >> "${dir}/args.log"
cat "${dir}/$run.log"
exit "$(cat "${dir}/$run.rc" 2>/dev/null || echo 0)"
`;
    for (const arch of ["amd64", "arm64"]) writeFileSync(join(assets, `promcheck-linux-${arch}`), fakeBin, { mode: 0o755 });
    writeFileSync(join(assets, `${CORPUS}.tar.gz`), "bundle");

    // curl: release metadata lists the assets; an asset download copies the file to the -o target.
    writeFileSync(
      join(bin, "curl"),
      `#!/bin/bash
url= out=
while (($#)); do
  case $1 in
    -o) out=$2; shift ;;
    https://*) url=$1 ;;
  esac
  shift
done
case $url in
  */releases/tags/*)
    printf '{"assets":[{"name":"promcheck-linux-amd64","url":"https://assets/promcheck-linux-amd64"},'
    printf '{"name":"promcheck-linux-arm64","url":"https://assets/promcheck-linux-arm64"},'
    printf '{"name":"${CORPUS}.tar.gz","url":"https://assets/${CORPUS}.tar.gz"}]}' ;;
  https://assets/*) cp "${assets}/\${url##*/}" "$out" ;;
  *) exit 22 ;;
esac
`,
      { mode: 0o755 }
    );

    // buildkite-agent: records each call with the markdown it was given on stdin.
    writeFileSync(
      join(bin, "buildkite-agent"),
      `#!/bin/bash\n{ printf '== %s\\n' "$*"; cat; } >> "${join(dir, "agent.log")}"\n`,
      { mode: 0o755 }
    );
  });

  afterEach(() => rmSync(dir, { recursive: true, force: true }));

  /** Runs the script with the given control and test logs, returning what CI would see. */
  function run(control: Case[], tested: Case[], opts: { controlRc?: number; env?: Record<string, string> } = {}) {
    writeFileSync(join(dir, "control.log"), promcheckLog(control));
    writeFileSync(join(dir, "test.log"), promcheckLog(tested));
    if (opts.controlRc !== undefined) writeFileSync(join(dir, "control.rc"), String(opts.controlRc));
    const result = spawnSync("bash", [SCRIPT], {
      cwd: repo,
      env: {
        ...process.env,
        PATH: `${join(dir, "bin")}:${process.env.PATH}`,
        PROMETHEUS_COMPLIANCE_TMPDIR: join(dir, "tmp"),
        GH_TOKEN: "token",
        PROMCHECK_VER: VERSION,
        PROMCHECK_TEST_INSTANCE_TIMEOUT: "900",
        GITHUB_PR_TARGET_BRANCH: "main",
        BUILDKITE_BUILD_URL: "",
        BUILDKITE_JOB_ID: "",
        ...opts.env,
      },
      encoding: "utf8",
    });
    const report = join(dir, "tmp", "validate-prometheus-compliance-output", `@${CORPUS}-report.md`);
    const read = (f: string) => (existsSync(f) ? readFileSync(f, "utf8") : "");
    return {
      status: result.status,
      output: result.stdout + result.stderr,
      report: read(report),
      agent: read(join(dir, "agent.log")),
      controlHead: read(join(dir, "control.head")).trim(),
      args: read(join(dir, "args.log")),
    };
  }

  const STABLE: Case[] = [
    ["001", "match", "up"],
    ["002", "mismatch", "rate(http_requests_total[5m])"],
  ];

  test("no change reports no regressions in one sentence, without a details block", () => {
    const r = run(STABLE, STABLE);
    expect(r.status).toBe(0);
    expect(r.report.startsWith("<!-- promcheck-pr-report -->\n## Promcheck\n\n**No regressions**\n\nNo query compatibility changes detected.\n")).toBe(true);
    expect(r.report).not.toContain("<details>");
    expect(r.agent).toContain("== annotate --context ctx-validate-prometheus-compliance --style success");
  });

  test("a regression fails the job after publishing the report", () => {
    const r = run(STABLE, [
      ["001", "mismatch", "up"],
      ["002", "mismatch", "rate(http_requests_total[5m])"],
    ]);
    expect(r.status).toBe(1);
    expect(r.report).toContain("**Regression detected**\n\n1 regression, 0 improvements, 0 flaky cases\n");
    expect(r.report).toContain("<summary><strong>Show changed queries</strong></summary>");
    expect(r.report).toContain("### Regressions\n\n| Case | Result | Query |\n|---:|:---:|---|\n| 001 | `PASS -> FAIL` | `up` |\n");
    expect(r.report).not.toContain("### Improvements");
    expect(r.agent).toContain("--style error");
  });

  test("improvements alone report no regressions", () => {
    const r = run(STABLE, [
      ["001", "match", "up"],
      ["002", "match", "rate(http_requests_total[5m])"],
    ]);
    expect(r.status).toBe(0);
    expect(r.report).toContain("**No regressions**\n\n0 regressions, 1 improvement, 0 flaky cases\n");
    expect(r.report).toContain("| 002 | `FAIL -> PASS` | `rate(http_requests_total[5m])` |");
    expect(r.report).not.toContain("### Regressions");
    expect(r.agent).toContain("--style success");
  });

  test("flaky transitions are reported separately without failing the job", () => {
    const r = run(
      [["001", "match", "up"], ["002", "flaky", "rate(http_requests_total[5m])"]],
      [["001", "flaky", "up"], ["002", "match", "rate(http_requests_total[5m])"]]
    );
    expect(r.status).toBe(0);
    expect(r.report).toContain("**No regressions**\n\n0 regressions, 0 improvements, 2 flaky cases\n");
    expect(r.report).toContain("### Flaky");
    expect(r.report).toContain("| 001 | `PASS -> FLAKY` | `up` |");
    expect(r.report).toContain("| 002 | `FLAKY -> PASS` | `rate(http_requests_total[5m])` |");
    expect(r.report).not.toContain("### Regressions");
    expect(r.report).not.toContain("### Improvements");
  });

  test("error memos and flaky skips add up in the run summaries", () => {
    const r = run(STABLE, [
      ["001", "match", "up"],
      ["002", "error", "a"],
      ["003", "error", "b"],
      ["004", "error", "c"],
      ["005", "flaky", "d"],
    ]);
    expect(r.status).toBe(1);
    expect(r.output).toContain('"match":1,"mismatch":1');
    expect(r.output).toContain('"errors":3');
    expect(r.output).toContain('"flaky":1');
  });

  test("the binary gets the checkout, the timeout in seconds, and JSONL output", () => {
    const r = run(STABLE, STABLE);
    expect(r.status).toBe(0);
    const lines = r.args.trim().split("\n");
    expect(lines).toHaveLength(2);
    for (const args of lines) {
      expect(args).toContain("--elasticsearch ");
      expect(args).toContain("--elasticsearch-timeout 900s");
      expect(args).toContain("-o json");
    }
  });

  test("a regression next to a fix is reported even when the number of passing queries holds", () => {
    const r = run(
      [
        ["001", "match", "up"],
        ["002", "mismatch", "rate(http_requests_total[5m])"],
        ["003", "match", "sum(x)"],
      ],
      [
        ["001", "match", "up"],
        ["002", "match", "rate(http_requests_total[5m])"],
        ["003", "error", "sum(x)"],
      ]
    );
    expect(r.status).toBe(1);
    expect(r.report).toContain("**Regression detected**\n\n1 regression, 1 improvement, 0 flaky cases\n");
    expect(r.report).toContain("| 003 | `PASS -> ERROR` | `sum(x)` |");
    expect(r.report.indexOf("### Regressions")).toBeLessThan(r.report.indexOf("### Improvements"));
    expect(r.agent).toContain("--style error");
  });

  test("rows are sorted by case id within each section", () => {
    const r = run(
      [
        ["010", "match", "a"],
        ["002", "match", "b"],
        ["007", "mismatch", "c"],
        ["003", "mismatch", "d"],
      ],
      [
        ["010", "mismatch", "a"],
        ["002", "mismatch", "b"],
        ["007", "match", "c"],
        ["003", "match", "d"],
      ]
    );
    const rows = r.report.match(/^\| \d{3} \|/gm);
    expect(rows).toEqual(["| 002 |", "| 010 |", "| 003 |", "| 007 |"]);
  });

  test.each([
    ["match", "match", "stable"],
    ["match", "error4xx", "regression"],
    ["match", "error5xx", "regression"],
    ["error4xx", "match", "improvement"],
    ["error5xx", "match", "improvement"],
    ["error4xx", "error4xx", "stable"],
    ["error5xx", "error5xx", "stable"],
    ["error4xx", "error5xx", "regression"],
    ["error5xx", "error4xx", "improvement"],
    ["match", "flaky", "flaky"],
    ["error4xx", "flaky", "flaky"],
    ["error5xx", "flaky", "flaky"],
    ["flaky", "match", "flaky"],
    ["flaky", "error4xx", "flaky"],
    ["flaky", "error5xx", "flaky"],
    ["flaky", "flaky", "flaky"],
    ["match", "mismatch", "regression"],
    ["mismatch", "match", "improvement"],
    ["mismatch", "error4xx", "regression"],
    ["error4xx", "mismatch", "improvement"],
    ["error", "match", "improvement"],
    ["match", "error", "regression"],
    ["error", "error5xx", "stable"],
    ["error5xx", "error", "stable"],
    ["match", "filtered", "stable"],
    ["filtered", "error5xx", "stable"],
  ] as const)("%s -> %s is %s", (before, after, category) => {
    const r = run([["001", before, "up"]], [["001", after, "up"]]);
    expect(r.status, r.output).toBe(category === "regression" ? 1 : 0);
    if (category === "stable") {
      expect(r.report).not.toContain("| 001 |");
      expect(r.report).not.toContain("<details>");
    } else {
      const section = category === "regression" ? "Regressions" : category === "improvement" ? "Improvements" : "Flaky";
      expect(r.report).toContain(`### ${section}`);
      expect(r.report).toContain("| 001 |");
    }
    expect(r.agent).toContain(category === "regression" ? "--style error" : "--style success");
    expect(r.agent).toContain(r.report.trim());
  });

  test("summary errors sum HTTP 4xx, HTTP 5xx, and non-HTTP errors", () => {
    const cases: Case[] = [["001", "error4xx", "a"], ["002", "error5xx", "b"], ["003", "error", "c"]];
    const r = run(cases, cases);
    expect(r.status).toBe(0);
    expect(r.output.match(/"errors":3/g)).toHaveLength(2);
    expect(r.report).not.toContain("<details>");
  });

  test("the comparison links the exact Buildkite job and promcheck release, with short commits", () => {
    const head = git(repo, "rev-parse", "HEAD");
    const r = run(STABLE, STABLE, {
      env: { BUILDKITE_BUILD_URL: "https://buildkite.com/elastic/elasticsearch-pull-request/builds/42", BUILDKITE_JOB_ID: "job-7" },
    });
    expect(r.report).toContain(`**Compared** \`${r.controlHead.slice(0, 10)}\` -> \`${head.slice(0, 10)}\`  \n`);
    expect(r.report).toContain(
      `[Buildkite run](https://buildkite.com/elastic/elasticsearch-pull-request/builds/42#job-7) | ` +
        `[promcheck v${VERSION}](https://github.com/elastic/promcheck/releases/tag/v${VERSION})\n`
    );
  });

  test("without a build there is no Buildkite link", () => {
    const r = run(STABLE, STABLE);
    expect(r.report).not.toContain("Buildkite run");
    expect(r.report.trimEnd().endsWith(`[promcheck v${VERSION}](https://github.com/elastic/promcheck/releases/tag/v${VERSION})`)).toBe(true);
  });

  test("the same runs give the same report", () => {
    const tested: Case[] = [
      ["001", "mismatch", "up"],
      ["002", "match", "rate(http_requests_total[5m])"],
    ];
    expect(run(STABLE, tested).report).toBe(run(STABLE, tested).report);
  });

  /** A fix landing on the target after the PR branched off must not count as this PR's change. */
  test("the control run builds the merge base, not the tip of the target branch", () => {
    const branchPoint = git(repo, "merge-base", "main", "pr");
    const other = join(dir, "other");
    git(dir, "clone", "-q", join(dir, "origin.git"), "other");
    const tip = commit(other, "later-on-main");
    git(other, "push", "-q", "origin", "main");

    const r = run(STABLE, STABLE);
    expect(r.status).toBe(0);
    expect(r.controlHead).toBe(branchPoint);
    expect(r.controlHead).not.toBe(tip);
  });

  test("the PR comment meta-data carries the same report as the annotation", () => {
    const r = run(STABLE, [
      ["001", "mismatch", "up"],
      ["002", "mismatch", "rate(http_requests_total[5m])"],
    ]);
    // The start note annotates first; the final pair carries the report.
    const parts = r.agent.split(/^== .*$/m);
    const annotation = parts[parts.length - 2];
    const comment = parts[parts.length - 1];
    expect(r.agent).toContain("== meta-data set pr_comment:validate-prometheus-compliance:body");
    expect(comment.trim()).toBe(annotation.trim());
    expect(comment.trim()).toBe(r.report.trim());
  });

  test("a pipe in a query is escaped and a backtick widens the code span", () => {
    const r = run(
      [
        ["001", "match", 'up{job=~"a|b"}'],
        ["002", "match", 'label_replace(up, "x", "`y`", "", "")'],
      ],
      [
        ["001", "mismatch", 'up{job=~"a|b"}'],
        ["002", "mismatch", 'label_replace(up, "x", "`y`", "", "")'],
      ]
    );
    expect(r.report).toContain('| 001 | `PASS -> FAIL` | `up{job=~"a\\|b"}` |');
    expect(r.report).toContain('| 002 | `PASS -> FAIL` | `` label_replace(up, "x", "`y`", "", "") `` |');
  });

  test("JSON query escapes round-trip without doubling backslashes or making extra rows", () => {
    const query = 'sum(\nup{job=~"a\\\\.b"})';
    const r = run([["001", "match", query]], [["001", "mismatch", query]]);
    expect(r.status).toBe(1);
    expect(r.report).toContain(query.replace(/\n/g, " "));
    expect(r.report.match(/^\| \d{3} \|/gm)).toHaveLength(1);
  });

  test("more changes than the table holds link the full list as an artifact", () => {
    const ids = Array.from({ length: 52 }, (_, i) => String(i + 1).padStart(3, "0"));
    const r = run(
      ids.map((id) => [id, "match", `m${id}`] as Case),
      ids.map((id) => [id, "mismatch", `m${id}`] as Case)
    );
    expect(r.report.match(/^\| \d{3} \|/gm)).toHaveLength(50);
    expect(r.report).toContain(`_2 more in <a href="artifact://${join(dir, "tmp").slice(1)}/validate-prometheus-compliance-output/@${CORPUS}-changes.tsv">`);
  });

  test("a control run that crashes is reported as not compared without failing the job", () => {
    const r = run(STABLE, STABLE, { controlRc: 2 });
    expect(r.status, r.output).toBe(0);
    expect(r.output).toContain("Not compared: The control run failed");
    expect(r.agent).toContain("== annotate --context ctx-validate-prometheus-compliance --style warning");
    expect(r.agent).toContain(
      "<!-- promcheck-pr-report -->\n## Promcheck\n\n**Not compared**\n\nThe control run failed.\n\n" +
        `[promcheck v${VERSION}](https://github.com/elastic/promcheck/releases/tag/v${VERSION})\n`
    );
    expect(r.agent).toContain("== meta-data set pr_comment:validate-prometheus-compliance:body");
    expect(r.report).toBe("");
  });

  test("a misconfigured pipeline fails the job", () => {
    const r = run(STABLE, STABLE, { env: { PROMCHECK_TEST_INSTANCE_TIMEOUT: "soon" } });
    expect(r.status).toBe(1);
    expect(r.output).toContain("PROMCHECK_TEST_INSTANCE_TIMEOUT must be a positive integer: soon");
    expect(r.agent).toBe("");
  });

  test("the report is ASCII only", () => {
    const r = run(
      [
        ["001", "match", "up"],
        ["002", "mismatch", "rate(http_requests_total[5m])"],
      ],
      [
        ["001", "mismatch", "up"],
        ["002", "match", "rate(http_requests_total[5m])"],
      ],
      { env: { BUILDKITE_BUILD_URL: "https://buildkite.com/elastic/elasticsearch-pull-request/builds/42", BUILDKITE_JOB_ID: "job-7" } }
    );
    expect(r.report).toMatch(/^[\x00-\x7F]+$/);
  });
});
