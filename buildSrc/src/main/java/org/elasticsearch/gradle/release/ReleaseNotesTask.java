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
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.VersionProperties;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.gradle.util.Util.capitalize;

/**
 * Implements a Gradle task that can generate release notes for a given Elasticsearch
 * release. Run it with:
 *
 * <pre>./gradlew buildReleaseNotes</pre>
 *
 * <p>You can override the Elasticsearch version and output path:
 *
 * <pre>./gradlew :buildReleaseNotes  --release-version 7.8.0 --release-output path/to/notes.asciidoc</pre>
 *
 * <p>See {@link ReleaseToolsPlugin} for this is wired up.
 */
public class ReleaseNotesTask extends DefaultTask {
    private static final Logger LOGGER = Logging.getLogger(ReleaseNotesTask.class);
    private static final String BASE_URL = "https://api.github.com/repos/";
    private static final String USER_REPO = "elastic/elasticsearch/";

    /**
     * Declares the labels that are used to categorise changes in the release notes. This is
     * <code>List</code> rather than a <code>Set</code> because the order of items in the
     * collection dictates the order that the corresponding changes appear in the notes.
     */
    private static final List<String> GROUPS = List.of(
        // Changing the order here will change the order in which PRs appear in the notes.
        ">breaking",
        ">breaking-java",
        ">deprecation",
        ">feature",
        ">enhancement",
        ">bug",
        ">regression",
        ">upgrade"
    );

    /**
     * Any change with a label in this list will be omitted from the release notes.
     */
    private static final Set<String> IGNORE = Set.of(
        ">non-issue",
        ">refactoring",
        ">docs",
        ">test",
        ">test-failure",
        ">test-mute",
        ":Core/Infra/Build",
        "backport",
        "WIP"
    );

    /**
     * These mappings translate {@link #GROUPS} labels into the headings as they should appears in the release notes.
     */
    private static final Map<String, String> GROUP_LABELS;

    /**
     * Issues are categorised by the area of the change, as well as the type of change. This is derived
     * from the team label, but for some labels the automatic derivation isn't right.
     */
    private static final Map<String, String> AREA_OVERRIDES;

    /**
     * This is just an inversion of {@link #AREA_OVERRIDES}, and is used to automatically trim
     * area prefixes from PR titles.
     */
    private static final Map<String, String> AREA_OVERRIDES_REVERSE_LOOKUP;

    static {
        GROUP_LABELS = new HashMap<>();

        GROUP_LABELS.put(">breaking", "Breaking changes");
        GROUP_LABELS.put(">breaking-java", "Breaking Java changes");
        GROUP_LABELS.put(">deprecation", "Deprecations");
        GROUP_LABELS.put(">feature", "New features");
        GROUP_LABELS.put(">enhancement", "Enhancements");
        GROUP_LABELS.put(">bug", "Bug fixes");
        GROUP_LABELS.put(">regression", "Regressions");
        GROUP_LABELS.put(">upgrade", "Upgrades");
        GROUP_LABELS.put("other", "NOT CLASSIFIED");

        AREA_OVERRIDES = new HashMap<>();

        AREA_OVERRIDES.put("ml", "Machine Learning");
        AREA_OVERRIDES.put("Beats", "Beats Plugin");
        AREA_OVERRIDES.put("Docs", "Docs Infrastructure");

        AREA_OVERRIDES_REVERSE_LOOKUP = new HashMap<>();
        AREA_OVERRIDES.forEach((label, override) -> AREA_OVERRIDES_REVERSE_LOOKUP.put(override, label));
    }

    /**
     * Used to parse JSON responses from GitHub.
     */
    private ObjectMapper objectMapper;

    /**
     * Holds the version for which release notes will be generated. Defaults to {@link VersionProperties#getElasticsearchVersion()}
     * but can be overridden via <code>--release-version</code> on the command line.
     */
    private Version version = null;

    /**
     * Holds the output path for the release notes, if one has been specified, otherwise the default is
     * <code>docs/reference/release-notes/{version}.asciidoc</code>
     */
    private String outputPath = null;

    /**
     * Used for writing the release notes.
     */
    private PrintStream out = null;

    /**
     * Holds the value of the <code>Authorization</code> header, when a GitHub key is successfully loaded.
     */
    private String basicAuthPayload = null;

    @Option(option = "release-version", description = "Override the version for generating release notes")
    public void setVersion(String overrideVersion) {
        this.version = Version.fromString(overrideVersion);
    }

    @Input
    public String getVersion() {
        return this.version.toString();
    }

    @Option(option = "release-output", description = "Override the output file path for the generated release notes")
    public void setOutput(String overrideOutputPath) {
        this.outputPath = overrideOutputPath;
    }

    @Input
    public String getOutput() {
        return this.outputPath;
    }

    @TaskAction
    public void executeTask() throws IOException {
        loadGitHubKey();

        this.objectMapper = new ObjectMapper();

        if (this.version == null) {
            this.version = VersionProperties.getElasticsearchVersion();
        }

        final Set<String> versionLabels = fetchVersionLabels();

        assert versionLabels.contains("v" + this.version);

        final Map<String, Map<String, List<Issue>>> groupedIssues = fetchIssues();

        final String output = this.outputPath != null ? this.outputPath : "docs/reference/release-notes/" + this.version + ".asciidoc";

        this.out = new PrintStream(output);

        generateReleaseNotes(groupedIssues);
    }

    private void generateReleaseNotes(Map<String, Map<String, List<Issue>>> groupedIssues) {
        generateHeader();

        List<String> groups = new ArrayList<>(GROUPS);
        groups.add("other");

        for (String group : groups) {
            if (groupedIssues.containsKey(group) == false) {
                continue;
            }

            generateGroupHeader(group);

            final Map<String, List<Issue>> issuesByHeader = groupedIssues.get(group);

            issuesByHeader.forEach((header, issues) -> {
                out.println((header.isEmpty() ? "HEADER MISSING" : header) + "::");

                for (Issue issue : issues) {
                    String title = prepareIssueTitle(header, issue);

                    out.print("* " + title + " {pull}" + issue.number + "[#" + issue.number + "]");

                    if (issue.relatedIssues.isEmpty() == false) {
                        out.print(issue.relatedIssues.size() == 1 ? " (issue: " : " (issues: ");
                        out.print(issue.relatedIssues.stream().map(i -> "{issue}" + i + "[#" + i + "]").collect(Collectors.joining(", ")));
                        out.print(")");
                    }
                    out.println();
                }
                out.println();
            });

            out.println();
            out.println();
        }
    }

    private String prepareIssueTitle(String header, Issue issue) {
        String title = issue.title;

        // Remove redundant prefixes from the title. For example,
        // given:
        //
        // SQL: add support for foo queries
        //
        // the prefix is redundant under the "SQL" section.
        String headerPrefix = AREA_OVERRIDES_REVERSE_LOOKUP.getOrDefault(header, header);
        title = title.replaceFirst("^\\[(?i)" + headerPrefix + "]\\s+", "");
        title = title.replaceFirst("^(?i)" + headerPrefix + ":\\s+", "");

        // Remove any issue number prefix
        title = title.replaceFirst("^#\\d+:?\\s+", "");

        title = capitalize(title);

        if (issue.state == Issue.State.OPEN) {
            title += " [OPEN]";
        }

        if (issue.isPullRequest == false) {
            title += " [ISSUE]";
        }
        return title;
    }

    private void generateGroupHeader(String group) {
        String groupId = group.substring(1);

        out.println("[[" + groupId + "-" + this.version + "]]");
        out.println("[float]");
        out.println("=== " + GROUP_LABELS.get(group));
        out.println();
    }

    private void generateHeader() {
        String branch = this.version.getMajor() + "." + this.version.getMinor();

        out.println(":issue: https://github.com/" + USER_REPO + "issues/");
        out.println(":pull:  https://github.com/" + USER_REPO + "pull/");
        out.println();
        out.println("[[release-notes-" + this.version + "]]");
        out.println("== {es} version " + this.version);
        out.println();
        out.println("coming[" + this.version + "]");
        out.println();
        out.println("Also see <<breaking-changes-" + branch + ",Breaking changes in " + branch + ">>.");
        out.println();
    }

    private void loadGitHubKey() throws IOException {
        final Path keyPath = Path.of(System.getenv("HOME"), ".elastic", "github_auth");
        LOGGER.debug("Attempting to load API key from {}", keyPath);

        if (Files.notExists(keyPath)) {
            LOGGER.warn(
                "File ~/.elastic/github_auth doesn't exist - using anonymous API. "
                    + "Generate a Personal Access Token at https://github.com/settings/applications"
            );
        }

        final String keyString = Files.readString(keyPath).trim();

        if (keyString.matches("^[0-9a-fA-F]{40}$") == false) {
            throw new GradleException("Invalid GitHub key: " + keyString);
        }

        this.basicAuthPayload = "Basic "
            + Base64.getEncoder().encodeToString((keyString + ":x-oauth-basic").getBytes(StandardCharsets.UTF_8));
    }

    private Set<String> fetchVersionLabels() throws IOException {
        LOGGER.quiet("Fetching GitHub labels...");

        final Set<String> versionLabels = new HashSet<>();

        int page = 0;

        while (true) {
            page++;

            final URL url = new URL(BASE_URL + USER_REPO + "labels?page=" + page);
            final JsonNode labels = fetchUrl(url);

            if (labels.isArray() == false) {
                throw new GradleException("Expect JSON array from GitHub, but received: " + labels.getNodeType());
            }

            if (labels.isEmpty()) {
                break;
            }

            for (JsonNode label : labels) {
                String name = label.get("name").asText();
                if (name.startsWith("v")) {
                    versionLabels.add(name);
                }
            }
        }

        return versionLabels;
    }

    private Map<String, Map<String, List<Issue>>> fetchIssues() throws IOException {
        final List<Issue> issues = new ArrayList<>();

        Set<Integer> seen = new HashSet<>();

        String currentVersionLabel = "v" + this.version;

        for (String state : List.of("open", "closed")) {
            int page = 0;

            while (true) {
                page++;

                URL url = new URL(
                    BASE_URL + USER_REPO + "issues?labels=" + currentVersionLabel + "&pagesize=100&state=" + state + "&page=" + page
                );

                final JsonNode tranche = fetchUrl(url);

                if (tranche.isArray() == false) {
                    throw new GradleException("Expect JSON array from GitHub, but received: " + tranche.getNodeType());
                }

                if (tranche.isEmpty()) {
                    break;
                }

                for (JsonNode jsonIssue : tranche) {

                    Set<String> labels = new HashSet<>();
                    for (JsonNode labelJson : jsonIssue.get("labels")) {
                        labels.add(labelJson.get("name").asText());
                    }

                    Issue issue = new Issue(
                        jsonIssue.get("number").asInt(),
                        jsonIssue.get("title").asText(),
                        jsonIssue.get("body").asText(),
                        labels,
                        jsonIssue.has("pull_request"),
                        jsonIssue.get("state").asText()
                    );

                    issues.add(issue);

                    if (issue.isPullRequest) {
                        Pattern pattern = Pattern.compile("(?:#|" + USER_REPO + "issues/)(\\d+)");

                        final Matcher matcher = pattern.matcher(issue.body);

                        while (matcher.find()) {
                            final Integer referencedIssue = Integer.parseInt(matcher.group(1));
                            seen.add(referencedIssue);

                            issue.relatedIssues.add(referencedIssue);
                        }
                    }
                }
            }
        }

        final Map<String, Map<String, List<Issue>>> groupedIssues = new HashMap<>();

        ISSUE: for (Issue issue : issues) {
            if (seen.contains(issue.number) && issue.isPullRequest == false) {
                continue;
            }

            for (String label : issue.labels) {
                if (IGNORE.contains(label)) {
                    continue ISSUE;
                }

                if (isPrReleasedInEarlierVersion(issue)) {
                    continue ISSUE;
                }
            }

            List<String> areaLabels = new ArrayList<>();
            for (String label : issue.labels) {
                if (label.startsWith(":")) {
                    String areaOfInterest = label.substring(1);
                    if (areaOfInterest.contains("/")) {
                        areaOfInterest = areaOfInterest.substring(areaOfInterest.indexOf("/") + 1);
                    }
                    areaLabels.add(areaOfInterest);
                }
            }

            String header = "NOT CLASSIFIED";

            if (areaLabels.size() > 1) {
                header = "MULTIPLE AREA LABELS";
            } else if (areaLabels.size() == 1) {
                final String areaLabel = areaLabels.get(0);
                header = AREA_OVERRIDES.getOrDefault(areaLabel, areaLabel);
            }

            for (String group : GROUPS) {
                if (issue.labels.contains(group)) {
                    Map<String, List<Issue>> issuesByHeader = groupedIssues.computeIfAbsent(group, _group -> new TreeMap<>());
                    issuesByHeader.computeIfAbsent(header, (_header) -> new ArrayList<>()).add(issue);

                    continue ISSUE;
                }
            }
            // else if not grouped:
            Map<String, List<Issue>> issuesByHeader = groupedIssues.computeIfAbsent("other", _group -> new TreeMap<>());
            issuesByHeader.computeIfAbsent(header, (_header) -> new ArrayList<>()).add(issue);
        }

        return groupedIssues;
    }

    private boolean isPrReleasedInEarlierVersion(Issue issue) {
        final Set<String> versionLabels = issue.getVersionLabels();

        final boolean isReleasingNewMajorSeries = this.version.getMinor() == 0 && this.version.getRevision() == 0;

        // We assume that if we're releasing the first version in a major
        // series, there does not (yet) exist any later major series, and any
        // other release versions are for a prior major. We should therefore
        // skip this PR as being already released.
        if (isReleasingNewMajorSeries && versionLabels.size() > 1) {
            return true;
        }

        // E.g. "v7."
        String currentVersionPrefix = "v" + this.version.getMajor() + ".";

        List<Version> sortableVersions = new ArrayList<>();

        for (String label : versionLabels) {
            // We filter by the current major, because we might release a change
            // at roughly the same time to a major series and the prior major
            // series. A user shouldn't have to consult release notes for the
            // prior major in order to see all the relevant changes.
            if (label.startsWith(currentVersionPrefix) == false) {
                continue;
            }

            sortableVersions.add(Version.fromString(label.substring(1)));
        }

        Collections.sort(sortableVersions);

        final Version earliestVersion = sortableVersions.get(0);

        return earliestVersion.equals(this.version) == false;
    }

    private JsonNode fetchUrl(URL url) throws IOException {
        LOGGER.debug("Fetching {}", url);

        // Connect to the web server endpoint
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();

        // Set HTTP method as GET
        urlConnection.setRequestMethod("GET");

        if (this.basicAuthPayload != null) {
            // Include the HTTP Basic Authentication payload
            urlConnection.addRequestProperty("Authorization", basicAuthPayload);
        }

        // Read response from web server, which will trigger HTTP Basic Authentication request to be sent.
        try (InputStream httpResponseReader = urlConnection.getInputStream()) {
            final byte[] content = IOUtils.toByteArray(httpResponseReader);
            return this.objectMapper.readTree(content);
        }
    }

    private static class Issue {
        private enum State {
            OPEN,
            CLOSED
        }

        private final int number;
        private final String title;
        private final String body;
        private final Set<String> labels;
        private final boolean isPullRequest;
        private final Set<Integer> relatedIssues;
        private final State state;

        public Issue(int number, String title, String body, Set<String> labels, boolean isPullRequest, String state) {
            this.number = number;
            this.title = title;
            this.body = body;
            this.labels = labels;
            this.isPullRequest = isPullRequest;
            this.state = State.valueOf(state.toUpperCase());

            this.relatedIssues = new TreeSet<>();
        }

        public Set<String> getVersionLabels() {
            return this.labels.stream().filter(label -> label.matches("^v\\d+\\.\\d+\\.\\d+$")).collect(Collectors.toSet());
        }
    }
}
