import glob
import os
import re
import subprocess
import yaml
from pprint import pp

# os.environ["GITHUB_PR_LABELS"] = "release_note:skip,:Delivery/Packaging,v8.11.0"
# os.environ["GITHUB_PR_TARGET_BRANCH"] = "main"
# os.environ[
#     "GITHUB_PR_TRIGGER_COMMENT"
# ] = "hey run elasticsearch-ci/build-benchmarks please and run elasticsearch-ci/part-2"

defaults = {}
with open(".buildkite/pipelines/pull-request/.defaults.yml", "r") as stream:
    defaults = yaml.safe_load(stream) or {}
    defaults["config"] = defaults.get("config") or {}

pipelines = []
files = glob.glob(".buildkite/pipelines/pull-request/*.yml")
for file in files:
    with open(file, "r") as stream:
        pipeline = yaml.safe_load(stream) or {}
        pipeline["config"] = {**defaults["config"], **(pipeline.get("config") or {})}
        pipelines.append(pipeline)
        name = os.path.basename(file).split(".", 1)[0]
        pipeline["name"] = name
        pipeline["config"]["trigger-phrase"] = (
            pipeline["config"].get("trigger-phrase")
            or f".*run\W+elasticsearch-ci/{name}.*"
        )

labels = os.environ.get("GITHUB_PR_LABELS", "").split(",")
labels = map(lambda x: x.strip(), labels)
labels = list(filter(lambda x: x, labels))

merge_base = (
    subprocess.check_output(
        ["git", "merge-base", os.environ["GITHUB_PR_TARGET_BRANCH"], "HEAD"]
    )
    .decode("utf-8")
    .strip()
)

# Grabbed changed files using `git diff --name-only $(git merge-base "$GITHUB_PR_TARGET_BRANCH" HEAD)`
changed_files_output = subprocess.check_output(
    [
        "git",
        "diff",
        "--name-only",
        merge_base,
    ]
)
changed_files = changed_files_output.decode("utf-8").split("\n")
changed_files = map(lambda x: x.strip(), changed_files)
changed_files = list(filter(lambda x: x, changed_files))


def get_list(str_or_list):
    return [str_or_list] if isinstance(str_or_list, str) else str_or_list


def label_check_allow(pipeline):
    if pipeline["config"].get("allow-labels"):
        return any(
            label in labels for label in get_list(pipeline["config"]["allow-labels"])
        )
    return True


def label_check_skip(pipeline):
    if pipeline["config"].get("skip-labels"):
        return not any(
            label in labels for label in get_list(pipeline["config"]["skip-labels"])
        )

    return True


# Exclude the pipeline if all of the changed files in the PR are in at least one excluded region
def changed_files_excluded_check(pipeline):
    if pipeline["config"].get("excluded-regions"):
        return not all(
            any(
                re.search(region, file)
                for region in get_list(pipeline["config"]["excluded-regions"])
            )
            for file in changed_files
        )
    return True


# Include the pipeline if all of the changed files in the PR are in at least one included region
def changed_files_included_check(pipeline):
    if pipeline["config"].get("included-regions"):
        return all(
            any(
                re.search(region, file)
                for region in get_list(pipeline["config"]["included-regions"])
            )
            for file in changed_files
        )
    return True


def trigger_comment_check(pipeline):
    if os.environ.get("GITHUB_PR_TRIGGER_COMMENT") and pipeline["config"].get(
        "trigger-phrase"
    ):
        return re.search(
            pipeline["config"]["trigger-phrase"],
            os.environ.get("GITHUB_PR_TRIGGER_COMMENT"),
        )
    return False


all_pipelines = [*pipelines]

for f in [
    label_check_allow,
    label_check_skip,
    changed_files_excluded_check,
    changed_files_included_check,
]:
    pipelines = list(filter(f, pipelines))

comment_triggered_pipelines = list(filter(trigger_comment_check, all_pipelines))

# Append any comment triggered pipelines that are not already in the list
for pipeline in comment_triggered_pipelines:
    if not any(p["name"] == pipeline["name"] for p in pipelines):
        pipelines.append(pipeline)

yaml.Dumper.ignore_aliases = lambda *args: True

# Remove our custom attributes before outputting the Buildkite YAML
for pipeline in pipelines:
    pipeline.pop("config", None)
    pipeline.pop("name", None)

print(yaml.dump(pipelines, default_flow_style=False))
