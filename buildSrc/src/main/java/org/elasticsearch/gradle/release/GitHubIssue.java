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

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

class GitHubIssue {
    public static GitHubIssue fromJson(JsonNode jsonIssue) {
        Set<String> labels = new HashSet<>();
        for (JsonNode labelJson : jsonIssue.get("labels")) {
            labels.add(labelJson.get("name").asText());
        }

        return new GitHubIssue(
            jsonIssue.get("number").asInt(),
            jsonIssue.get("title").asText(),
            jsonIssue.get("body").asText(),
            labels,
            jsonIssue.has("pull_request"),
            jsonIssue.get("state").asText()
        );
    }

    public enum State {
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

    public GitHubIssue(int number, String title, String body, Set<String> labels, boolean isPullRequest, String state) {
        this.number = number;
        this.title = title;
        this.body = body;
        this.labels = labels;
        this.isPullRequest = isPullRequest;
        this.state = State.valueOf(state.toUpperCase());

        this.relatedIssues = new TreeSet<>();
    }

    public int getNumber() {
        return number;
    }

    public String getTitle() {
        return title;
    }

    public String getBody() {
        return body;
    }

    public Set<String> getLabels() {
        return labels;
    }

    public boolean isPullRequest() {
        return isPullRequest;
    }

    public Set<Integer> getRelatedIssues() {
        return relatedIssues;
    }

    public State getState() {
        return state;
    }

    public Set<String> getVersionLabels() {
        return this.labels.stream().filter(label -> label.matches("^v\\d+\\.\\d+\\.\\d+$")).collect(Collectors.toSet());
    }
}
