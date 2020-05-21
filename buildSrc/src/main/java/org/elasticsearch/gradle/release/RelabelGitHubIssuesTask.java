/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle.release;

import com.fasterxml.jackson.databind.JsonNode;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.Collections.emptySet;

/**
 * This task finds issues in GitHub, by matching on one or more label, then adds and / or
 * removes one or more labels. This is useful code-freezing for a release, as anything
 * that has missed the cut-off needs to retarget the next release.
 *
 * <p>For example, to move issues and PRs from the v7.8.0 to the next patch release, run:
 *
 * <pre>./gradlew relabelGithubIssues --labels v7.8.0 --remove v7.8.0  --add v7.8.1</pre>
 *
 * <p>While the default repository it <code>elastic/elasticsearch</code>, you can override
 * this using the <code>relabel.repository</code> system property:
 *
 * <pre>./gradlew -Drelabel.repository="foo/bar" relabelGithubIssues # etc</pre>
 *
 * <p>See {@link ReleaseToolsPlugin} for this is wired up.
 */
public class RelabelGitHubIssuesTask extends DefaultTask {
    private static final Logger LOGGER = Logging.getLogger(RelabelGitHubIssuesTask.class);

    private static final String BASE_URL = "https://api.github.com/repos";
    private static final String USER_REPO = System.getProperty("relabel.repository", "elastic/elasticsearch");
    private static final String ISSUE_URL = "https://github.com/" + USER_REPO + "/issues";

    private State labelState = State.open;
    private Set<String> matchLabels = emptySet();
    private Set<String> addLabels = emptySet();
    private Set<String> removeLabels = emptySet();
    private GitHubApi githubApi;
    private boolean simulate;

    /** Represents the state of the GitHub issues to match */
    private enum State {
        open,
        closed,
        all
    }

    @Option(option = "state", description = "State of GitHub issues to relabel: open|closed|all. Default: open")
    public void setLabelState(State labelState) {
        this.labelState = labelState;
    }

    @Input
    public State getLabelState() {
        return this.labelState;
    }

    @Option(option = "labels", description = "CSV string of labels for matching GitHub issues to relabel.")
    public void setMatchLabels(String labels) {
        this.matchLabels = Set.of(labels.split(","));
    }

    @Input
    public String getMatchLabels() {
        return String.join(",", this.matchLabels);
    }

    @Option(option = "add", description = "Labels to add to matching GitHub issues. CSV string")
    public void setAddLabels(String labels) {
        this.addLabels = Set.of(labels.split(","));
    }

    @Input
    public String getAddLabels() {
        return String.join(",", this.addLabels);
    }

    @Option(option = "remove", description = "Labels to remove from matching GitHub issues. CSV string")
    public void setRemoveLabels(String labels) {
        this.removeLabels = Set.of(labels.split(","));
    }

    @Input
    public String getRemoveLabels() {
        return String.join(",", this.removeLabels);
    }

    @Option(option = "simulate", description = "Don't actually change anything, but print what actions would have been taken")
    public void setSimulate(boolean simulate) {
        this.simulate = simulate;
    }

    @Input
    public boolean isSimulate() {
        return simulate;
    }

    @TaskAction
    public void executeTask() throws IOException {
        if (labelState == null) {
            throw new GradleException("Must specify a label state to match. One of: open|closed|all");
        }

        if (matchLabels.isEmpty()) {
            throw new GradleException("Must specify comma-separated list of labels to match via --labels");
        }

        if (addLabels.isEmpty() && removeLabels.isEmpty()) {
            throw new GradleException("Must specify comma-separated list of labels to add and/or remove via --add or --remove");
        }

        this.githubApi = new GitHubApi(simulate);

        this.relabel();
    }

    private void relabel() {
        final Spool<GitHubIssue> issueIds = fetchIssues();

        LOGGER.quiet("Processing issues...");
        for (GitHubIssue issue : issueIds) {
            // We print out the URL for the issue in the GitHub UI here, as opposed to the API URL
            LOGGER.quiet(ISSUE_URL + "/" + issue.getNumber());

            if (this.addLabels.isEmpty() == false) {
                addLabels(issue);
            }

            if (this.removeLabels.isEmpty() == false) {
                removeLabels(issue);
            }
        }

        LOGGER.quiet("Done");
    }

    /**
     * @see <a href="https://developer.github.com/v3/issues/labels/#add-labels-to-an-issue"
     * >GitHub API - Add labels to an issue</a>
     * @param issue the issue to edit
     */
    private void addLabels(GitHubIssue issue) {
        String url = BASE_URL + "/" + USER_REPO + "/issues/" + issue.getNumber() + "/labels";
        this.githubApi.post(url, Map.of("labels", this.addLabels));
    }

    /**
     * @see <a href="https://developer.github.com/v3/issues/labels/#remove-a-label-from-an-issue"
     * >GitHub API - Remove a label from an issue</a>
     * @param issue the issue to edit
     */
    private void removeLabels(GitHubIssue issue) {
        for (String eachLabel : this.removeLabels) {
            // GitHub will fail the request if you try to delete a label from
            // an issue, and that issue doesn't have that label.
            if (issue.getLabels().contains(eachLabel)) {
                String url = BASE_URL + "/" + USER_REPO + "/issues/" + issue.getNumber() + "/labels/" + encodeValue(eachLabel);
                this.githubApi.delete(url);
            }
        }
    }

    /**
     * @see <a href="https://developer.github.com/v3/issues/#list-repository-issues"
     * >GitHub API - List repository issues</a>
     */
    private Spool<GitHubIssue> fetchIssues() {
        final Map<String, Object> params = new HashMap<>();
        params.put("sort", "created");
        params.put("direction", "asc");
        params.put("state", this.labelState);
        params.put("labels", String.join(",", this.matchLabels));
        params.put("per_page", 100);
        // We also add "page" below before we fetch the URL

        LOGGER.quiet(String.format(Locale.ROOT, "Fetching %s issues labelled %s...", this.labelState, this.matchLabels));

        String issuesUrl = BASE_URL + "/" + USER_REPO + "/issues?" + asQueryString(params);

        // We increment the page number for every call. The spool will stop
        // fetching pages when no results are returned.
        final AtomicInteger page = new AtomicInteger(1);

        final Spool<GitHubIssue> spool = new Spool<>(() -> {
            final JsonNode response = this.githubApi.get(issuesUrl + "&page=" + page.getAndIncrement());
            if (response.isArray() == false) {
                throw new GradleException("Expected JSON array response from GitHub");
            }

            List<GitHubIssue> ids = new ArrayList<>(response.size());
            for (JsonNode each : response) {
                ids.add(GitHubIssue.fromJson(each));
            }
            return ids;
        });

        return spool;
    }

    private String asQueryString(Map<String, Object> params) {
        return params.entrySet()
            .stream()
            .map(entry -> entry.getKey() + "=" + encodeValue(entry.getValue().toString()))
            .collect(Collectors.joining("&"));
    }

    private String encodeValue(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
