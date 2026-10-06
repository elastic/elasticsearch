import { execFileSync } from "child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join, resolve } from "path";
import { afterEach, beforeEach, describe, expect, test } from "vitest";

const SCRIPT = resolve(`${import.meta.dirname}/validate-prometheus-compliance.sh`);
const DATASET = "promql_test_queries-grafana";
const VERSION = "0.5.4";

type Status = "OK" | "FAIL" | "ERR";
type Case = [id: string, status: Status, expr: string];

/** A promcheck log as the script reads it: one block per case, then the summary table. Statuses are coloured, as in CI. */
function promcheckLog(cases: Case[]): string {
  const blocks = cases.map(([id, status, expr]) => `[${id}] \x1b[32m\x1b[1m${status}\x1b[0m\n  expr:      ${expr}\n  notes:     ${expr}\n`);
  const count = (s: Status) => cases.filter(([, status]) => status === s).length;
  return (
    blocks.join("") +
    `| executed.total   | ${cases.length} | 100% |\n` +
    `| executed.success | ${count("OK")} | |\n` +
    `| executed.failure | ${count("FAIL")} | |\n` +
    `| executed.error   | ${count("ERR")} | |\n`
  );
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

    // Release assets: the data tarball carries the query corpus the script looks for, the package is opaque.
    const assets = join(dir, "assets");
    mkdirSync(join(dir, "data-src", "data", "results"), { recursive: true });
    writeFileSync(join(dir, "data-src", "data", "results", `${DATASET}.jsonl`), '{"query":"up"}\n');
    mkdirSync(assets);
    execFileSync("tar", ["-czf", join(assets, `promcheck-data-${VERSION}.tar.gz`), "-C", join(dir, "data-src"), "data"]);
    writeFileSync(join(assets, `promcheck-${VERSION}.tar.gz`), "package");

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
    printf '{"assets":[{"name":"promcheck-data-${VERSION}.tar.gz","url":"https://assets/promcheck-data-${VERSION}.tar.gz"},'
    printf '{"name":"promcheck-${VERSION}.tar.gz","url":"https://assets/promcheck-${VERSION}.tar.gz"}]}' ;;
  https://assets/*) cp "${assets}/\${url##*/}" "$out" ;;
  *) exit 22 ;;
esac
`,
      { mode: 0o755 }
    );

    // uv: \`run ... promcheck --workspace W\` prints the canned log of the run the workspace stands for (the PR checkout
    // is the test run, anything else the control), records the commit it was built from, and exits with its code.
    writeFileSync(
      join(bin, "uv"),
      `#!/bin/bash
[[ $1 == run ]] || exit 0
while (($#)); do [[ $1 == --workspace ]] && { ws=$2; break; }; shift; done
[[ $(cd "$ws" && pwd -P) == $(pwd -P) ]] && run=test || run=control
git -C "$ws" rev-parse HEAD > "${dir}/$run.head"
cat "${dir}/$run.log"
exit "$(cat "${dir}/$run.rc" 2>/dev/null || echo 0)"
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
  function run(control: Case[], tested: Case[], opts: { controlRc?: number } = {}) {
    writeFileSync(join(dir, "control.log"), promcheckLog(control));
    writeFileSync(join(dir, "test.log"), promcheckLog(tested));
    if (opts.controlRc !== undefined) writeFileSync(join(dir, "control.rc"), String(opts.controlRc));
    let status = 0;
    let stderr = "";
    try {
      execFileSync("bash", [SCRIPT, DATASET], {
        cwd: repo,
        env: {
          ...process.env,
          PATH: `${join(dir, "bin")}:${process.env.PATH}`,
          PROMETHEUS_COMPLIANCE_TMPDIR: join(dir, "tmp"),
          GH_TOKEN: "token",
          PROMCHECK_VER: VERSION,
          PROMCHECK_TEST_INSTANCE_TIMEOUT: "900",
          BUILDKITE_PULL_REQUEST_BASE_BRANCH: "main",
        },
        stdio: "pipe",
      });
    } catch (e) {
      status = (e as { status: number }).status;
      stderr = String((e as { stderr: Buffer }).stderr);
    }
    const report = join(dir, "tmp", "validate-prometheus-compliance-output", `@${DATASET}-report.md`);
    const read = (f: string) => (existsSync(f) ? readFileSync(f, "utf8") : "");
    return {
      status,
      stderr,
      report: read(report),
      agent: read(join(dir, "agent.log")),
      controlHead: read(join(dir, "control.head")).trim(),
    };
  }

  const STABLE: Case[] = [
    ["001", "OK", "up"],
    ["002", "FAIL", "rate(http_requests_total[5m])"],
  ];

  test("unchanged coverage passes with a one-line success report and no table", () => {
    const r = run(STABLE, STABLE);
    expect(r.status).toBe(0);
    expect(r.report).toContain("ok 1 → 1 (+0)");
    expect(r.report).not.toContain("<details>");
    expect(r.agent).toContain("== annotate --context ctx-validate-prometheus-compliance --style success");
  });

  test("a regression fails the job, annotates an error and lists the case", () => {
    const r = run(STABLE, [
      ["001", "FAIL", "up"],
      ["002", "FAIL", "rate(http_requests_total[5m])"],
    ]);
    expect(r.status).toBe(1);
    expect(r.report).toContain("ok 1 → 0 (-1)");
    expect(r.report).toContain("<summary>1 regressed, 0 improved</summary>");
    expect(r.report).toContain("| 001 | OK → FAIL | `up` |");
    expect(r.agent).toContain("--style error");
  });

  test("an improvement passes and is listed after the regressions", () => {
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
    expect(r.report).toContain("<summary>1 regressed, 1 improved</summary>");
    expect(r.report.indexOf("| 003 | OK → ERR |")).toBeLessThan(r.report.indexOf("| 002 | FAIL → OK |"));
    expect(r.agent).toContain("--style success");
  });

  test("the summary links promcheck's source at its version and both commits", () => {
    const head = git(repo, "rev-parse", "HEAD");
    const r = run(STABLE, STABLE);
    expect(r.report).toContain(`[promcheck ${VERSION}](https://github.com/elastic/promcheck/tree/v${VERSION})`);
    expect(r.report).toContain(`(https://github.com/elastic/elasticsearch/commit/${head})`);
    expect(r.report).toContain(`(https://github.com/elastic/elasticsearch/commit/${r.controlHead})`);
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
    expect(r.agent).toContain("== meta-data set pr_comment:validate-prometheus-compliance:body");
    expect(comment.trim()).toBe(annotation.trim());
    expect(comment.trim()).toBe(r.report.trim());
  });

  test("a pipe in a query is escaped and a long query is cut short", () => {
    const long = `sum(rate(${"x".repeat(200)}[5m]))`;
    const r = run(
      [
        ["001", "OK", 'up{job=~"a|b"}'],
        ["002", "OK", long],
      ],
      [
        ["001", "FAIL", 'up{job=~"a|b"}'],
        ["002", "FAIL", long],
      ]
    );
    expect(r.report).toContain('`up{job=~"a\\|b"}`');
    expect(r.report).toContain(`\`${long.slice(0, 157)}...\``);
  });

  test("more changes than the table holds link the full list as an artifact", () => {
    const ids = Array.from({ length: 52 }, (_, i) => String(i + 1).padStart(3, "0"));
    const r = run(
      ids.map((id) => [id, "OK", `m${id}`] as Case),
      ids.map((id) => [id, "FAIL", `m${id}`] as Case)
    );
    expect(r.report.match(/^\| \d{3} \|/gm)).toHaveLength(50);
    expect(r.report).toContain(`_2 more in <a href="artifact://${join(dir, "tmp").slice(1)}/validate-prometheus-compliance-output/@${DATASET}-changes.tsv">`);
  });

  test("a control run that crashes fails the job without a report", () => {
    const r = run(STABLE, STABLE, { controlRc: 2 });
    expect(r.status).toBe(1);
    expect(r.stderr).toContain("control run failed");
    expect(r.agent).toBe("");
  });
});
