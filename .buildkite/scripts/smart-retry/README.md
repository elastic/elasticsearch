# Smart Retry + Spot Preemption handling

## Overview

Smart retry handles automatic retries for tasks/tests in two different scenarios:

- When a test or task fails
- When a test or task is cancelled or never run due to spot preemption

The basic idea is that when a Buildkite step fails, or is cancelled due to the spot instance being terminated, we need to retry the step, but skip all of the successful work we did in the previous test.

- If a non-spot failure passes on a retry, it was a flaky failure.
- If a spot failure succeeds on the retry, the original failure can be completely ignored.

There are a lot of edge cases. You could have a scenario like:

- Attempt #1 - Task A and Task B fail before the spot preemption. Task C and Task D are cancelled due to Spot preemption
- Attempt #2 - Task A succeeds, Task B and C are cancelled due to spot preemption
- Attempt #3 - Task B,D suceed and C fails
- Attempt #4 - Task C fails, Task D succeeds

All 4 attempts have to be thought of as a single, combined run to understand what the end result is. The end result that is relevant to the user is:

- Task A and Task B are flaky
- Task C is a real, consistent failure
- Task D is successful

## Process

Here's the overall process that makes this work, ignoring spot preemptions for now:

- For any given run, if it is a retry, download the `task-status` from the previous step, and have Gradle skip all tests/tasks that completed successfully across previous attempts.
- As Gradle executes tasks and tests, keep track of their status (not started, started, finished successfully, finished with an error)
- After everything is finished executing, have Gradle write a `task-status` file with the final status of all tests and tasks.
- Combine this file with the `task-status` from the previous attempt, if there was one, so that each attempt has a cumulative view of all the previous attempts.
- Upload this file as a Buildkite artifact (which the next attempt, if there is one, will download)
- Automatically retry this step if there were any failures for tests that haven't executed twice

### Spot Preemptions

Here's how the above process additionally handles spot preemptions:

- GCP instances have an internal, HTTP-based metadata server you can query from the instance. There is a metadata item that gets set when the instance has been preempted. 30 seconds after this flag is set, the instance will be terminated.
- Have Gradle poll this metadata item every 1s checking for preemption status
- If the preemption flag flips to true, Gradle should immediately stop executing tests/tasks.
- Mark all tests/tasks that were executing at the time of cancellation as "cancelled", then write the `task-status` file as usual.
- Tag the buildscan as `PREEMPTED` and also add a value for the preemption time. Upload the buildscan as normal.
- Combine/upload task-status as normal
- Exit with a special exit code ($GCP_PREEMPTION_EXIT_CODE - currently 47) to mark this step as "Preempted"
- Automatically retry this step based on that exit code

In the subsequent retry, any tests/tasks that didn't fully complete before the preemption (or never ran at all), will execute.

## Relevant Source

- build-tools-internal/src/main/java/org/elasticsearch/gradle/internal/ci/GcpPreemptionWatchdog.java
  -  Polls instance metadata service to watch for preemption status
- build-tools-internal/src/main/java/org/elasticsearch/gradle/internal/ci/PreemptionBuildCanceller.java
  - Handles cancelling the build when the instance gets preempted. Cancels running tasks, child processes, etc.
- build-tools-internal/src/main/java/org/elasticsearch/gradle/internal/ci/TaskStatusReport.java
  - Writes the `task-status` report at the end of execution
- build-tools-internal/src/main/java/org/elasticsearch/gradle/internal/ci/TaskStatusTrackerPlugin.java
  - Tracks status of tasks as they start/finish
- build-tools-internal/src/main/java/org/elasticsearch/gradle/internal/test/rerun/InternalTestRerunPlugin.java
  - Handles filtering out successful tests/tasks from retry runs
- .buildkite/scripts/smart-retry
  - All of the non-Gradle bits. Downloading previous reports, combining reports, putting together the retry data to pass to Gradle, summarizing overall status at the end of execution for the user.
