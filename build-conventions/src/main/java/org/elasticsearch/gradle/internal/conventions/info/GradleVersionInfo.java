/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.conventions.info;

import java.net.URL;
import java.util.List;
import java.util.Map;
import groovy.json.JsonSlurper;
import java.net.MalformedURLException;
import java.util.Objects;
import java.util.stream.Collectors;

public class GradleVersionInfo {

    static Version getLatestRelease() throws MalformedURLException{
        URL url = new URL("https://services.gradle.org/versions/current");
        JsonSlurper slurper = new JsonSlurper();
        Map parsedJson = (Map) slurper.parse(url);
        return new Version((String) parsedJson.get("version"), "", "", false);
    }

    static Version getLatestRC() throws MalformedURLException {
        URL url = new URL("https://services.gradle.org/versions/release-candidate");
        JsonSlurper slurper = new JsonSlurper();
        Map parsedJson = (Map)slurper.parse(url);
        return new Version((String)parsedJson.get("version"),
                (String)parsedJson.get("rcFor"),
                (String)parsedJson.get("milestoneFor"),
                false
                );
    }

    static Version getLatestReleaseOrRC() throws MalformedURLException{
        Version latestRelease = getLatestRelease();
        Version latestRC = getLatestRC();
        return latestRC.getRcFor().equals(latestRelease.getVersionString()) ? latestRelease : latestRC;
    }

    static class Version {
        private final String versionString;
        private final String rcFor;
        private final String milestoneFor;
        private final boolean snapshot;

        Version(String versionString, String rcFor, String milestoneFor, boolean snapshot) {
            this.versionString = versionString;
            this.rcFor = rcFor;
            this.milestoneFor = milestoneFor;
            this.snapshot = snapshot;
        }

        public String getVersionString() {
            return versionString;
        }

        public boolean isFinal() {
            return snapshot == false && (rcFor == null || rcFor.isEmpty()) && (milestoneFor == null || milestoneFor.isEmpty());
//            return rcFor != null && (rcFor.isEmpty() == false) || (milestoneFor != null && milestoneFor.isEmpty() == false);
        }

        public String getRcFor() {
            return rcFor;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Version version = (Version) o;
            return snapshot == version.snapshot && Objects.equals(versionString, version.versionString) && Objects.equals(rcFor, version.rcFor) && Objects.equals(milestoneFor, version.milestoneFor);
        }

        @Override
        public int hashCode() {
            return Objects.hash(versionString, rcFor, milestoneFor, snapshot);
        }
    }

    static List<Version> getGradleVersions() throws MalformedURLException {
        URL url = new URL("https://services.gradle.org/versions/all");
        JsonSlurper slurper = new JsonSlurper();
        List<Map> parsedJson = (List<Map>) slurper.parse(url);
        return parsedJson.stream().map(m -> new Version((String)m.get("version"),
                        (String)m.get("rcFor"),
                        (String)m.get("milestoneFor"),
                        (boolean)m.get("snapshot")))
                .collect(Collectors.toList());
    }

    static Version getPreviousRelease(Version version) throws MalformedURLException{
        List<Version> collect = getGradleVersions().stream()
                .filter(v -> v.isFinal())
                .sorted((v1, v2) -> v2.versionString.compareTo(v1.versionString))
                .collect(Collectors.toList());
        return (version.isFinal()) ?  collect.get(collect.indexOf(version) + 1) : collect.get(0);
    }

}
