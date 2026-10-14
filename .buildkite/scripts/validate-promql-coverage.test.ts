import { execFileSync, spawnSync } from "child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join, resolve } from "path";
import { afterEach, beforeEach, describe, expect, test } from "vitest";

const SCRIPT = resolve(`${import.meta.dirname}/validate-promql-coverage.sh`);
const CORPUS = "grafanalabs-top100";
const VERSION = "0.9.1+prometheus-3.13.1";

type Status = "OK" | "FAIL" | "ERR" | "SKIP";
type Case = [id: string, status: Status, expr: string];

/** A promcheck log as the script reads it: one block per case, then the verdict line. Statuses are coloured, as in CI. */
function promcheckLog(cases: Case[]): string {
  const blocks = cases.map(([id, status, expr]) => `\n[${id}] \x1b[32m\x1b[1m${status}\x1b[0m\n  expr:      ${expr}\n  notes:     ${expr}\n`);
  const count = (s: Status) => cases.filter(([, status]) => status === s).length;
  const errs = count("ERR");
  // Spread errors over the memos so the summary parser meets each shape.
  const e4 = Math.floor((errs + 2) / 3);
  const e5 = Math.floor((errs + 1) / 3);
  const ex = Math.floor(errs / 3);
  const skip = count("SKIP");
  return (
    blocks.join("") +
    `${cases.length} inspected: ${count("OK")} match, ${count("FAIL")} mismatch, ${e4} 4xx, ${e5} 5xx` +
    (ex ? `, ${ex} error` : "") +
    (skip ? `, ${skip} flaky` : "") +
    "\n"
  );
}

describe("validate-promql-coverage.sh", () => {
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
        PROMQL_COVERAGE_TMPDIR: join(dir, "tmp"),
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
    const report = join(dir, "tmp", "validate-promql-coverage-output", `@${CORPUS}-report.md`);
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
    ["001", "OK", "up"],
    ["002", "FAIL", "rate(http_requests_total[5m])"],
  ];

  test("no change reports no regressions in one sentence, without a details block", () => {
    const r = run(STABLE, STABLE);
    expect(r.status).toBe(0);
    expect(r.report.startsWith("<!-- promcheck-pr-report -->\n## Promcheck\n\n**No regressions**\n\nNo query compatibility changes detected.\n")).toBe(true);
    expect(r.report).not.toContain("<details>");
    expect(r.agent).toContain("== annotate --context ctx-validate-promql-coverage --style success");
  });

  test("a regression is reported without failing the job, listing only the regressions", () => {
    const r = run(STABLE, [
      ["001", "FAIL", "up"],
      ["002", "FAIL", "rate(http_requests_total[5m])"],
    ]);
    expect(r.status).toBe(0);
    expect(r.report).toContain("**Regression detected**\n\n1 regression, 0 fixes, 1 changed case\n");
    expect(r.report).toContain("<summary><strong>Show changed queries</strong></summary>");
    expect(r.report).toContain("### Regressions\n\n| Case | Result | Query |\n|---:|:---:|---|\n| 001 | `PASS -> FAIL` | `up` |\n");
    expect(r.report).not.toContain("### Fixed");
    expect(r.agent).toContain("--style error");
  });

  test("fixes alone report no regressions and list only the fixes", () => {
    const r = run(STABLE, [
      ["001", "OK", "up"],
      ["002", "OK", "rate(http_requests_total[5m])"],
    ]);
    expect(r.status).toBe(0);
    expect(r.report).toContain("**No regressions**\n\n0 regressions, 1 fix, 1 changed case\n");
    expect(r.report).toContain("| 002 | `FAIL -> PASS` | `rate(http_requests_total[5m])` |");
    expect(r.report).not.toContain("### Regressions");
    expect(r.agent).toContain("--style success");
  });

  test("a flaky skip is a transition like any other", () => {
    const r = run(
      [
        ["001", "OK", "up"],
        ["002", "SKIP", "rate(http_requests_total[5m])"],
      ],
      [
        ["001", "SKIP", "up"],
        ["002", "OK", "rate(http_requests_total[5m])"],
      ]
    );
    expect(r.status).toBe(0);
    expect(r.report).toContain("**Regression detected**\n\n1 regression, 1 fix, 2 changed cases\n");
    expect(r.report).toContain("| 001 | `PASS -> SKIP` | `up` |");
    expect(r.report).toContain("| 002 | `SKIP -> PASS` | `rate(http_requests_total[5m])` |");
  });

  test("error memos and flaky skips add up in the run summaries", () => {
    const r = run(STABLE, [
      ["001", "OK", "up"],
      ["002", "ERR", "a"],
      ["003", "ERR", "b"],
      ["004", "ERR", "c"],
      ["005", "SKIP", "d"],
    ]);
    expect(r.status).toBe(0);
    expect(r.output).toContain("Merge base: ok=1 fail=1 err=0 skip=0 total=2");
    expect(r.output).toContain("PR: ok=1 fail=0 err=3 skip=1 total=5");
  });

  test("the binary gets the checkout, the timeout in seconds, and verbose detail blocks", () => {
    const r = run(STABLE, STABLE);
    expect(r.status).toBe(0);
    const lines = r.args.trim().split("\n");
    expect(lines).toHaveLength(2);
    for (const args of lines) {
      expect(args).toContain("--elasticsearch ");
      expect(args).toContain("--elasticsearch-timeout 900s");
      expect(args).toContain("--verbose");
    }
  });

  test("a regression next to a fix is reported even when the number of passing queries holds", () => {
    const r = run(
      [
        ["001", "OK", "up"],
        ["002", "FAIL", "rate(http_requests_total[5m])"],
        ["003", "OK", "sum(x)"],
      ],
      [
        ["001", "OK", "up"],
        ["002", "OK", "rate(http_requests_total[5m])"],
        ["003", "ERR", "sum(x)"],
      ]
    );
    expect(r.status).toBe(0);
    expect(r.report).toContain("**Regression detected**\n\n1 regression, 1 fix, 2 changed cases\n");
    expect(r.report).toContain("| 003 | `PASS -> ERROR` | `sum(x)` |");
    expect(r.report.indexOf("### Regressions")).toBeLessThan(r.report.indexOf("### Fixed"));
    expect(r.agent).toContain("--style error");
  });

  test("rows are sorted by case id within each section", () => {
    const r = run(
      [
        ["010", "OK", "a"],
        ["002", "OK", "b"],
        ["007", "FAIL", "c"],
        ["003", "FAIL", "d"],
      ],
      [
        ["010", "FAIL", "a"],
        ["002", "FAIL", "b"],
        ["007", "OK", "c"],
        ["003", "OK", "d"],
      ]
    );
    const rows = r.report.match(/^\| \d{3} \|/gm);
    expect(rows).toEqual(["| 002 |", "| 010 |", "| 003 |", "| 007 |"]);
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
      ["001", "FAIL", "up"],
      ["002", "OK", "rate(http_requests_total[5m])"],
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
      ["001", "FAIL", "up"],
      ["002", "FAIL", "rate(http_requests_total[5m])"],
    ]);
    const [, annotation, comment] = r.agent.split(/^== .*$/m);
    expect(r.agent).toContain("== meta-data set pr_comment:validate-promql-coverage:body");
    expect(comment.trim()).toBe(annotation.trim());
    expect(comment.trim()).toBe(r.report.trim());
  });

  test("a pipe in a query is escaped and a backtick widens the code span", () => {
    const r = run(
      [
        ["001", "OK", 'up{job=~"a|b"}'],
        ["002", "OK", 'label_replace(up, "x", "`y`", "", "")'],
      ],
      [
        ["001", "FAIL", 'up{job=~"a|b"}'],
        ["002", "FAIL", 'label_replace(up, "x", "`y`", "", "")'],
      ]
    );
    expect(r.report).toContain('| 001 | `PASS -> FAIL` | `up{job=~"a\\|b"}` |');
    expect(r.report).toContain('| 002 | `PASS -> FAIL` | `` label_replace(up, "x", "`y`", "", "") `` |');
  });

  test("more changes than the table holds link the full list as an artifact", () => {
    const ids = Array.from({ length: 52 }, (_, i) => String(i + 1).padStart(3, "0"));
    const r = run(
      ids.map((id) => [id, "OK", `m${id}`] as Case),
      ids.map((id) => [id, "FAIL", `m${id}`] as Case)
    );
    expect(r.report.match(/^\| \d{3} \|/gm)).toHaveLength(50);
    expect(r.report).toContain(`_2 more in <a href="artifact://${join(dir, "tmp").slice(1)}/validate-promql-coverage-output/@${CORPUS}-changes.tsv">`);
  });

  test("a control run that crashes is reported as not compared without failing the job", () => {
    const r = run(STABLE, STABLE, { controlRc: 2 });
    expect(r.status).toBe(0);
    expect(r.output).toContain("Not compared: The control run failed");
    expect(r.agent).toContain("== annotate --context ctx-validate-promql-coverage --style warning");
    expect(r.agent).toContain(
      "<!-- promcheck-pr-report -->\n## Promcheck\n\n**Not compared**\n\nThe control run failed.\n\n" +
        `[promcheck v${VERSION}](https://github.com/elastic/promcheck/releases/tag/v${VERSION})\n`
    );
    expect(r.agent).toContain("== meta-data set pr_comment:validate-promql-coverage:body");
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
        ["001", "OK", "up"],
        ["002", "FAIL", "rate(http_requests_total[5m])"],
      ],
      [
        ["001", "FAIL", "up"],
        ["002", "OK", "rate(http_requests_total[5m])"],
      ],
      { env: { BUILDKITE_BUILD_URL: "https://buildkite.com/elastic/elasticsearch-pull-request/builds/42", BUILDKITE_JOB_ID: "job-7" } }
    );
    expect(r.report).toMatch(/^[\x00-\x7F]+$/);
  });
});
