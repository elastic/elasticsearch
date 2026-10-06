#!/usr/bin/env bash
set -euo pipefail

prog=${0##*/}
job=${prog%.sh}
work=/tmp/$job
input=$work/input
output=/tmp/$job-output
cache=$work/.cache
artifact=promcheck.tar.gz
control=
annotation_context=ctx-promql-coverage-report
# Changed cases listed per kind in the report; the full list is the changes artifact.
max_rows=50

die() { printf '%s: %s\n' "$prog" "$*" >&2; exit 1; }
log() { printf -- '--- %s\n' "$*" >&2; }

counts() {
	awk -F'|' '
		function trim(s) { gsub(/[[:space:]]/, "", s); return s }
		/^\|[[:space:]]*executed\.success[[:space:]]*\|/ { ok = trim($3) }
		/^\|[[:space:]]*executed\.failure[[:space:]]*\|/ { fail = trim($3) }
		/^\|[[:space:]]*executed\.error[[:space:]]*\|/   { err = trim($3) }
		/^\|[[:space:]]*executed\.total[[:space:]]*\|/   { total = trim($3) }
		END {
			if (ok !~ /^[0-9]+$/ || fail !~ /^[0-9]+$/ || err !~ /^[0-9]+$/ || total !~ /^[0-9]+$/)
				exit 1
			skip = total - ok - fail - err
			if (skip < 0)
				exit 2
			print ok, fail, err, skip, total
		}
	' "$1" || die "failed to parse summary from $1"
}

# Every case of a promcheck log as "<case>\t<status>\t<expression>", colour codes stripped.
outcomes() {
	awk '
		{ gsub(/\033\[[0-9;]*m/, "") }
		/^\[[0-9]+\] [A-Z]+/ { id = substr($1, 2, length($1) - 2); status[id] = $2; order[++n] = id; next }
		n && /^[[:space:]]+expr:/ { e = $0; sub(/^[[:space:]]+expr:[[:space:]]*/, "", e); expr[order[n]] = e }
		END { for (i = 1; i <= n; i++) printf "%s\t%s\t%s\n", order[i], status[order[i]], expr[order[i]] }
	' "$1"
}

# Cases that pass in one run and not the other: "<regression|improvement>\t<case>\t<control>\t<test>\t<expression>".
changes() {
	awk -F'\t' '
		NR == FNR { before[$1] = $2; next }
		($1 in before) && ((before[$1] == "OK") != ($2 == "OK")) {
			printf "%s\t%s\t%s\t%s\t%s\n", ($2 == "OK" ? "improvement" : "regression"), $1, before[$1], $2, $3
		}
	' <(outcomes "$1") <(outcomes "$2")
}

# Changed cases as a markdown table, regressions first, at most $max_rows rows; $2 links the full list.
table() {
	local file=$1 link=$2
	printf '| case | change | query |\n|---|---|---|\n'
	sort -t "$(printf '\t')" -k1,1r -k2,2n "$file" | awk -F'\t' -v max="$max_rows" -v link="$link" '
		++n <= max {
			e = $5
			if (length(e) > 160) e = substr(e, 1, 157) "..."
			gsub(/`/, "'"'"'", e)
			gsub(/\|/, "\\|", e)
			printf "| %s | %s → %s | `%s` |\n", $2, $3, $4, e
		}
		END { if (n > max) printf "\n_%d more in %s._\n", n - max, link }
	'
}

# Shows the report on the build, the way the other CI reports do, and offers it to the build bot's PR comment.
# The PR comment only appears in pipelines that enable build bot comments (ELASTIC_PR_COMMENTS_ENABLED).
annotate() {
	local style=$1 file=$2
	command -v buildkite-agent >/dev/null || return 0
	buildkite-agent annotate --context "$annotation_context" --style "$style" < "$file" \
		|| log 'failed to annotate the build'
	buildkite-agent meta-data set "pr_comment:promql-coverage-report:body" < "$file" \
		|| log 'failed to set the PR comment meta-data'
}

cleanup() {
	[[ $control ]] || return
	git worktree remove --force "$control" >/dev/null 2>&1 || true
	rm -rf -- "$control"
}

main() {
	local dataset=${1:?missing dataset}
	local token=${GH_TOKEN:-${GITHUB_TOKEN:-}}
	local version=${PROMCHECK_VER:?missing required env: PROMCHECK_VER}
	local test_instance_timeout=${PROMCHECK_TEST_INSTANCE_TIMEOUT:?missing required env: PROMCHECK_TEST_INSTANCE_TIMEOUT}
	local python_version=3.14
	local branch=${BUILDKITE_PULL_REQUEST_BASE_BRANCH:?missing required env: BUILDKITE_PULL_REQUEST_BASE_BRANCH}
	local tag=v$version release url
	local data=$work/promcheck-data.tar.gz
	local pkg=$work/$artifact
	local data_dir=$work/data
	local fixture=$input/$dataset.jsonl
	local control_log=$output/@$dataset-control.log
	local test_log=$output/@$dataset-test.log
	local src dst n=0 rc base
	local c_ok c_fail c_err c_skip c_total
	local t_ok t_fail t_err t_skip t_total
	local delta status revision regressions improvements
	local repo_url=https://github.com/elastic/elasticsearch
	local changes_tsv=$output/@$dataset-changes.tsv
	local report_md=$output/@$dataset-report.md
	# Artifacts keep their absolute path without the leading slash.
	local changes_link="<a href=\"artifact://${changes_tsv#/}\">${changes_tsv##*/}</a>"

	for cmd in curl find git jq tar tee uv; do
		command -v "$cmd" >/dev/null || die "missing: $cmd"
	done
	[[ $token ]] || die 'missing GH_TOKEN or GITHUB_TOKEN'
	[[ $test_instance_timeout =~ ^[0-9]+$ ]] || die "PROMCHECK_TEST_INSTANCE_TIMEOUT must be an integer: $test_instance_timeout"
	((test_instance_timeout > 0)) || die "PROMCHECK_TEST_INSTANCE_TIMEOUT must be greater than 0: $test_instance_timeout"

	rm -rf -- "$work" "$output"
	mkdir -p -- "$input" "$output"

	release=$(
		curl -fsSL --retry 3 --retry-delay 2 \
			-H 'Accept: application/vnd.github+json' \
			-H "Authorization: Bearer $token" \
			"https://api.github.com/repos/elastic/promcheck/releases/tags/$tag"
	) || die "failed to fetch release metadata: elastic/promcheck@$tag"

	log "downloading: elastic/promcheck@$tag/promcheck-data-$version.tar.gz"

	url=$(jq -er --arg name "promcheck-data-$version.tar.gz" '.assets[] | select(.name == $name) | .url' <<<"$release") \
		|| die "release asset not found: promcheck-data-$version.tar.gz"
	curl -fsSL --retry 3 --retry-delay 2 \
		-H "Authorization: Bearer $token" \
		-H 'Accept: application/octet-stream' \
		"$url" -o "$data" \
		|| die "can't download asset: promcheck-data-$version.tar.gz"
	mkdir -p -- "$data_dir"
	tar -xzf "$data" -C "$data_dir"

	log "downloading: elastic/promcheck@$tag/promcheck-$version.tar.gz"
	url=$(jq -er --arg name "promcheck-$version.tar.gz" '.assets[] | select(.name == $name) | .url' <<<"$release") \
		|| die "release asset not found: promcheck-$version.tar.gz"
	curl -fsSL --retry 3 --retry-delay 2 \
		-H "Authorization: Bearer $token" \
		-H 'Accept: application/octet-stream' \
		"$url" -o "$pkg" \
		|| die "can't download asset: promcheck-$version.tar.gz"

	while IFS= read -r -d '' src; do
		dst=$input/${src##*/}
		[[ ! -e $dst ]] || die "duplicate query corpus filename: ${src##*/}"
		cp -- "$src" "$dst"
		((n += 1))
	done < <(find "$data_dir" -type f -path '*/data/results/*' -name '*.jsonl' -print0)

	((n)) || die "can't find query corpus files under data/results"
	[[ -f $fixture ]] || die "query corpus file not found: $dataset.jsonl"
	[[ -f $pkg ]] || die "promcheck artifact not found: $pkg"

	trap cleanup EXIT

	log "fetching: $branch"
	git fetch --no-tags origin "$branch"
	# The control is where this PR branched off, not the tip of $branch: a PromQL change that landed on
	# $branch since would otherwise count as this PR's regression or improvement.
	base=$(git merge-base "origin/$branch" HEAD) || die "can't find the merge base with origin/$branch"
	log "control: $base"
	control=$(mktemp -d "/tmp/$job-control-XXXXXX")
	git worktree add --detach "$control" "$base"
	uv python install "$python_version"

	log 'running control'
	set +e
	NO_COLOR=1 uv run --cache-dir "$cache" --python "$python_version" --with "$pkg" promcheck \
		--granularity 15s \
		--test-instance-timeout "$test_instance_timeout" \
		--workspace "$control" \
		"$fixture" 2>&1 | tee "$control_log"
	rc=${PIPESTATUS[0]}
	set -e
	if ((rc != 0 && rc != 1)); then
		die 'control run failed'
	fi

	log 'running test'
	set +e
	NO_COLOR=1 uv run --cache-dir "$cache" --python "$python_version" --with "$pkg" promcheck \
		--granularity 15s \
		--test-instance-timeout "$test_instance_timeout" \
		--workspace "$PWD" \
		"$fixture" 2>&1 | tee "$test_log"
	rc=${PIPESTATUS[0]}
	set -e
	if ((rc != 0 && rc != 1)); then
		die 'test run failed'
	fi

	read -r c_ok c_fail c_err c_skip c_total < <(counts "$control_log")
	read -r t_ok t_fail t_err t_skip t_total < <(counts "$test_log")

	delta=$((t_ok - c_ok))
	if ((delta < 0)); then
		status=regression
	elif ((delta > 0)); then
		status=improvement
	else
		status=stable
	fi

	log "$status: ok=$c_ok fail=$c_fail err=$c_err skip=$c_skip total=$c_total -> ok=$t_ok fail=$t_fail err=$t_err skip=$t_skip total=$t_total delta_ok=${delta:+$delta}"

	revision=$(git rev-parse HEAD)
	changes "$control_log" "$test_log" > "$changes_tsv"
	regressions=$(grep -c '^regression' "$changes_tsv" || true)
	improvements=$(grep -c '^improvement' "$changes_tsv" || true)
	{
		printf '**PromQL coverage** · [promcheck %s](https://github.com/elastic/promcheck/tree/v%s) · ' "$version" "$version"
		printf '[`%s`](%s/commit/%s) → [`%s`](%s/commit/%s) · ok %s → %s (%+d)\n' \
			"${base:0:10}" "$repo_url" "$base" "${revision:0:10}" "$repo_url" "$revision" "$c_ok" "$t_ok" "$delta"
		if ((regressions + improvements)); then
			printf '\n<details><summary>%d regressed, %d improved</summary>\n\n' "$regressions" "$improvements"
			table "$changes_tsv" "$changes_link"
			printf '\n</details>\n'
		fi
	} > "$report_md"
	if ((delta < 0)); then
		annotate error "$report_md"
	else
		annotate success "$report_md"
	fi

	((delta >= 0))
}

main "$@"
