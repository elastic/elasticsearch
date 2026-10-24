import { execFileSync, spawnSync } from "child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join, resolve } from "path";
import { afterEach, beforeEach, describe, expect, test } from "vitest";

const SCRIPT = resolve(`${import.meta.dirname}/validate-prometheus-compliance.sh`);
const CORPUS = "grafanalabs-top100";
const JQ = execFileSync("sh", ["-c", "command -v jq"], { encoding: "utf8" }).trim();
const VERSION = "0.11.0+prometheus-3.13.1";

type Status = "match" | "mismatch" | "error" | "error4xx" | "error5xx" | "flaky" | "filtered";
type Case = [id: string, status: Status, expr: string];

/** JSONL records matching promcheck --output json, with the full expression. */
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
printf '%s diagnostics\\n' "$run" >&2
cat "${dir}/$run.log"
exit "$(cat "${dir}/$run.rc" 2>/dev/null || echo 0)"
`;
    for (const arch of ["amd64", "arm64"]) writeFileSync(join(assets, `promcheck-linux-${arch}`), fakeBin, { mode: 0o755 });
    writeFileSync(join(assets, `${CORPUS}.tar.gz`), "bundle");

    // curl: release metadata lists the assets; an asset download copies the file to the -o target.
    writeFileSync(
      join(bin, "curl"),
      `#!/bin/bash
printf '%s\\n' "$@" >> "${join(dir, "curl-args.log")}"
url= out=
while (($#)); do
  case $1 in
    -H) [[ $2 == @- ]] && cat >> "${join(dir, "curl-headers.log")}"; shift ;;
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
  function run(control: Case[], tested: Case[], opts: { controlRc?: number; testRc?: number; controlLog?: string; testLog?: string; env?: Record<string, string>; args?: string[] } = {}) {
    writeFileSync(join(dir, "control.log"), opts.controlLog ?? promcheckLog(control));
    writeFileSync(join(dir, "test.log"), opts.testLog ?? promcheckLog(tested));
    if (opts.controlRc !== undefined) writeFileSync(join(dir, "control.rc"), String(opts.controlRc));
    if (opts.testRc !== undefined) writeFileSync(join(dir, "test.rc"), String(opts.testRc));
    const result = spawnSync("bash", [SCRIPT, ...(opts.args ?? [])], {
      cwd: repo,
      env: {
        ...process.env,
        PATH: `${join(dir, "bin")}:${process.env.PATH}`,
        PROMETHEUS_COMPLIANCE_TMPDIR: join(dir, "tmp"),
        GH_TOKEN: "token",
        PROMCHECK_VER: VERSION,
        PROMCHECK_TEST_INSTANCE_TIMEOUT: "900",
        GRADLEW: GRADLEW,
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
      stdout: result.stdout,
      stderr: result.stderr,
      report: read(report),
      controlStderr: read(join(dir, "tmp", "validate-prometheus-compliance-output", `@${CORPUS}-control.stderr.log`)),
      testStderr: read(join(dir, "tmp", "validate-prometheus-compliance-output", `@${CORPUS}-test.stderr.log`)),
      controlLog: read(join(dir, "tmp", "validate-prometheus-compliance-output", `@${CORPUS}-control.log`)),
      changes: read(join(dir, "tmp", "validate-prometheus-compliance-output", `@${CORPUS}-changes.json`)),
      agent: read(join(dir, "agent.log")),
      controlHead: read(join(dir, "control.head")).trim(),
      args: read(join(dir, "args.log")),
    };
  }

  // The pre-command hook's GRADLEW, whose options the script reuses.
  const GRADLEW =
    "./gradlew --console=plain --parallel --scan --build-cache --no-watch-fs -Dorg.elasticsearch.build.cache.url=https://gradle-enterprise.elastic.co/cache/";

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
    const r = run([...STABLE, ["003", "match", "b"], ["004", "match", "c"], ["005", "match", "d"]], [
      ["001", "match", "up"],
      ["002", "error", "a"],
      ["003", "error", "b"],
      ["004", "error", "c"],
      ["005", "flaky", "d"],
    ]);
    expect(r.status).toBe(1);
    expect(r.output).toContain('"match":4,"mismatch":1');
    expect(r.output).toContain('"errors":3');
    expect(r.output).toContain('"flaky":1');
  });

  test("the binary gets the checkout, the timeout, JSONL output, the remote build cache, and a node sized for the agent", () => {
    const r = run(STABLE, STABLE);
    expect(r.status).toBe(0);
    const cpus = execFileSync("getconf", ["_NPROCESSORS_ONLN"]).toString().trim();
    const lines = r.args.trim().split("\n");
    expect(lines).toHaveLength(2);
    for (const args of lines) {
      expect(args).toContain("--elasticsearch ");
      expect(args).toContain("--elasticsearch-timeout 900s");
      expect(args).toContain("--output json");
      expect(args).toContain(`--concurrency ${cpus} --`);
      expect(args).toMatch(
        / -- --console=plain --parallel --scan --build-cache --no-watch-fs -Dorg\.elasticsearch\.build\.cache\.url=https:\/\/gradle-enterprise\.elastic\.co\/cache\/ --configuration-cache -I \S+\/\.ci\/init\.gradle /,
      );
      expect(args).toContain(` -Dtests.es.node.processors=${cpus} `);
      expect(args).toMatch(/ -Dtests\.heap\.size=[1-9]\d*m /);
      expect(args).toMatch(/ -Drun\.license_type=trial -Dtests\.es\.xpack\.ml\.enabled=false$/);
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
    expect(r.report).toContain(`_2 more in <a href="artifact://${join(dir, "tmp").slice(1)}/validate-prometheus-compliance-output/@${CORPUS}-changes.json">`);
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

  test.each(["control", "test"] as const)("malformed %s JSON produces Not compared", (side) => {
    const malformed = promcheckLog(STABLE) + "{broken\n";
    const r = run(STABLE, STABLE, side === "control" ? { controlLog: malformed } : { testLog: malformed });
    expect(r.status).toBe(0);
    expect(r.agent).toContain("**Not compared**");
    expect(r.report).toBe("");
  });

  test("a failed JSON field extraction produces Not compared", () => {
    writeFileSync(join(dir, "bin", "jq"), `#!/bin/bash
[[ $1 == -jn ]] && exit 5
exec "${JQ}" "$@"
`, { mode: 0o755 });
    const r = run(STABLE, STABLE);
    expect(r.status).toBe(0);
    expect(r.agent).toContain("**Not compared**");
    expect(r.agent).toContain("Could not compare promcheck results");
    expect(r.report).toBe("");
  });

  test("the JSON changes artifact preserves full query text", () => {
    const query = 'sum(\n\tup{job=~"a\\\\.b|c"})';
    const r = run([["001", "match", query]], [["001", "error4xx", query]]);
    expect(r.status).toBe(1);
    expect(JSON.parse(r.changes)).toEqual([{
      name: "001", before: "match", after: "error4xx", category: "regression", query,
    }]);
  });

  test("tee failure prevents a truncated log from being compared", () => {
    writeFileSync(join(dir, "bin", "tee"), "#!/bin/bash\ncat\nexit 1\n", { mode: 0o755 });
    const r = run(STABLE, STABLE);
    expect(r.status).toBe(0);
    expect(r.agent).toContain("Not compared");
    expect(r.agent).toContain("The control run failed");
    expect(r.report).toBe("");
  });

  test("promcheck exit 1 remains a valid comparison result", () => {
    const cases: Case[] = [["001", "error4xx", "up"]];
    const r = run(cases, cases, { controlRc: 1, testRc: 1 });
    expect(r.status).toBe(0);
    expect(r.report).toContain("No regressions");
  });

  test("GitHub authorization is passed on stdin instead of curl arguments", () => {
    const r = run(STABLE, STABLE, { env: { GH_TOKEN: "secret-test-value" } });
    expect(r.status).toBe(0);
    const args = readFileSync(join(dir, "curl-args.log"), "utf8");
    const headers = readFileSync(join(dir, "curl-headers.log"), "utf8");
    expect(args).not.toContain("secret-test-value");
    expect(args).not.toContain("Authorization:");
    expect(headers.match(/Authorization: Bearer secret-test-value/g)).toHaveLength(3);
  });

  test("fetch updates the target tracking branch even with a narrow clone refspec", () => {
    git(repo, "config", "remote.origin.fetch", "+refs/heads/other:refs/remotes/origin/other");
    git(repo, "update-ref", "-d", "refs/remotes/origin/main");
    const r = run(STABLE, STABLE);
    expect(r.status).toBe(0);
    expect(r.report).toContain("No regressions");
    expect(git(repo, "rev-parse", "origin/main")).toBe(r.controlHead);
  });

  test("a null query is rendered as empty text without failing jq", () => {
    const cases: Case[] = [["001", "match", "up"]];
    const changed: Case[] = [["001", "error4xx", "up"]];
    const testLog = promcheckLog(changed).replace('"query":"up"', '"query":null');
    const r = run(cases, changed, { testLog });
    expect(r.status).toBe(1);
    expect(r.report).toContain("| 001 | `PASS -> ERROR 4xx` | `` |");
  });

  test("cleanup runs when mktemp creates a directory but fails", () => {
    const orphan = join(dir, "orphan-control");
    writeFileSync(join(dir, "bin", "mktemp"), `#!/bin/bash\nmkdir -p "${orphan}"\nprintf '%s\\n' "${orphan}"\nexit 1\n`, { mode: 0o755 });
    const r = run(STABLE, STABLE);
    expect(r.status).not.toBe(0);
    expect(existsSync(orphan)).toBe(false);
    expect(git(repo, "worktree", "list", "--porcelain").match(/^worktree /gm)).toHaveLength(1);
  });

  test("cleanup removes the control worktree after an unhandled artifact write failure", () => {
    writeFileSync(join(dir, "bin", "jq"), `#!/bin/bash
[[ $1 == -Rs ]] && exit 2
exec "${JQ}" "$@"
`, { mode: 0o755 });
    const r = run([["001", "match", "up"]], [["001", "error4xx", "up"]]);
    expect(r.status).toBe(2);
    expect(r.agent).toContain("Report generation or setup failed (exit 2)");
    expect(r.agent).toContain("--style warning");
    expect(r.agent.lastIndexOf("**Not compared**")).toBeGreaterThan(r.agent.lastIndexOf("**Promcheck running**"));
    expect(git(repo, "worktree", "list", "--porcelain").match(/^worktree /gm)).toHaveLength(1);
    expect(r.report).not.toContain("No regressions");
  });

  test("cleanup prunes stale worktree metadata", () => {
    const stale = join(dir, "stale-worktree");
    git(repo, "worktree", "add", "--detach", stale, "main");
    rmSync(stale, { recursive: true });
    const r = run(STABLE, STABLE);
    expect(r.status).toBe(0);
    expect(git(repo, "worktree", "list", "--porcelain").match(/^worktree /gm)).toHaveLength(1);
  });

  test("annotation failures are diagnostics on stderr", () => {
    writeFileSync(join(dir, "bin", "buildkite-agent"), "#!/bin/bash\ncat >/dev/null\nexit 1\n", { mode: 0o755 });
    const r = run(STABLE, STABLE);
    expect(r.status).toBe(0);
    expect(r.stderr).toContain("Failed to annotate the build");
    expect(r.stderr).toContain("Failed to set the PR comment");
    expect(r.stdout).not.toContain("Failed to");
  });

  test("notify posts only the running note and asks the build bot to comment early", () => {
    const r = run(STABLE, STABLE, {
      args: ["notify"],
      env: {
        GH_TOKEN: "",
        GRADLEW: "",
        GITHUB_PR_TARGET_BRANCH: "",
        BUILDKITE_BUILD_URL: "https://buildkite.example/builds/7",
        BUILDKITE_JOB_ID: "notify-job",
      },
    });
    expect(r.status).toBe(0);
    expect(r.args).toBe("");
    expect(r.agent).toContain("== meta-data set pr_comment:validate-prometheus-compliance:body");
    expect(r.agent).toContain("<!-- promcheck-pr-report -->\n## Promcheck\n\n**Promcheck running**");
    // The note links the build, not the short-lived notify job.
    expect(r.agent).toContain("[Buildkite run](https://buildkite.example/builds/7) | ");
    expect(r.agent).not.toContain("#notify-job");
    expect(r.agent).toContain("== meta-data set pr_comment:early_comment_job_id notify-job");
    expect(r.agent.indexOf("pr_comment:early_comment_job_id")).toBeGreaterThan(
      r.agent.indexOf("pr_comment:validate-prometheus-compliance:body"),
    );
  });

  test("a misconfigured pipeline fails the job", () => {
    const r = run(STABLE, STABLE, { env: { PROMCHECK_TEST_INSTANCE_TIMEOUT: "soon" } });
    expect(r.status).toBe(1);
    expect(r.output).toContain("PROMCHECK_TEST_INSTANCE_TIMEOUT must be a positive integer: soon");
    expect(r.agent).toBe("");
    const noGradlew = run(STABLE, STABLE, { env: { GRADLEW: "" } });
    expect(noGradlew.status).toBe(1);
    expect(noGradlew.output).toContain("GRADLEW must be set");
    const badConcurrency = run(STABLE, STABLE, { env: { PROMCHECK_CONCURRENCY: "lots" } });
    expect(badConcurrency.status).toBe(1);
    expect(badConcurrency.output).toContain("PROMCHECK_CONCURRENCY must be a positive integer: lots");
  });

  test("PROMCHECK_CONCURRENCY overrides the CPU count for promcheck but not for the node", () => {
    const cpus = execFileSync("getconf", ["_NPROCESSORS_ONLN"]).toString().trim();
    const r = run(STABLE, STABLE, { env: { PROMCHECK_CONCURRENCY: "3" } });
    expect(r.status).toBe(0);
    for (const args of r.args.trim().split("\n")) {
      expect(args).toContain("--concurrency 3 --");
      expect(args).toContain(` -Dtests.es.node.processors=${cpus} `);
    }
  });

  test("stderr is streamed to the console and saved separately from JSONL", () => {
    const r = run(STABLE, STABLE, { controlRc: 2 });
    expect(r.status).toBe(0);
    expect(r.controlStderr).toBe("control diagnostics\n");
    expect(r.stderr).toContain("control diagnostics");
    expect(r.controlLog).not.toContain("diagnostics");
    for (const line of r.controlLog.trim().split("\n")) expect(() => JSON.parse(line)).not.toThrow();
  });

  test("both runs retain stderr artifacts", () => {
    const r = run(STABLE, STABLE);
    expect(r.controlStderr).toBe("control diagnostics\n");
    expect(r.testStderr).toBe("test diagnostics\n");
  });

  test("stderr tee failure prevents comparison", () => {
    writeFileSync(join(dir, "bin", "tee"), `#!/bin/bash
case "$2" in
  *.stderr.log) cat >/dev/null; exit 3 ;;
  *) exec /usr/bin/tee "$@" ;;
esac
`, { mode: 0o755 });
    const r = run(STABLE, STABLE);
    expect(r.status).toBe(0);
    expect(r.agent).toContain("The control run failed");
    expect(r.report).toBe("");
  });

  test.each(["control", "test"] as const)("unknown %s status cannot become stable", (side) => {
    const unknown = promcheckLog(STABLE).replace('"status":"match"', '"status":"future_status"');
    const r = run(STABLE, STABLE, side === "control" ? { controlLog: unknown } : { testLog: unknown });
    expect(r.status).toBe(0);
    expect(r.stderr).toContain("Unknown promcheck status: future_status");
    expect(r.agent).toContain("**Not compared**");
    expect(r.report).toBe("");
  });

  test("unknown skip reasons cannot silently filter a case", () => {
    const cases: Case[] = [["001", "flaky", "up"]];
    const testLog = promcheckLog(cases).replace('"skip_reason":"flaky"', '"skip_reason":"future_reason"');
    const r = run(cases, cases, { testLog });
    expect(r.stderr).toContain("Unknown promcheck skip reason");
    expect(r.agent).toContain("**Not compared**");
  });

  test.each([
    ["missing", [["001", "match", "up"]]],
    ["extra", [...STABLE, ["003", "match", "up"]]],
    ["replaced", [["001", "match", "up"], ["003", "match", "up"]]],
    ["duplicate", [["001", "match", "up"], ["001", "match", "up"]]],
  ] as [string, Case[]][])("%s test cases prevent comparison", (_, tested) => {
    const r = run(STABLE, tested);
    expect(r.status).toBe(0);
    expect(r.agent).toContain("**Not compared**");
    expect(r.report).toBe("");
  });

  test("duplicate control cases prevent comparison", () => {
    const r = run([STABLE[0], STABLE[0]], STABLE);
    expect(r.stderr).toContain("Duplicate control case");
    expect(r.agent).toContain("**Not compared**");
  });

  test("large reports cap displayed queries and retain every full query in the artifact", () => {
    const query = 'up{label="' + "|".repeat(1000) + '"}';
    const before: Case[] = [], after: Case[] = [];
    for (let i = 0; i < 150; i++) {
      const id = String(i + 1).padStart(3, "0");
      before.push([id, i < 50 ? "match" : i < 100 ? "mismatch" : "match", query]);
      after.push([id, i < 50 ? "mismatch" : i < 100 ? "match" : "flaky", query]);
    }
    const r = run(before, after);
    expect(r.status).toBe(1);
    expect(r.report.match(/^\| \d{3} \|/gm)).toHaveLength(150);
    expect(r.report.length).toBeLessThan(50000);
    expect(r.report).toContain("Queries are shortened to 200 characters");
    expect(r.report).toContain("...");
    expect(JSON.parse(r.changes)).toHaveLength(150);
    expect(JSON.parse(r.changes).every((row: { query: string }) => row.query === query)).toBe(true);
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
