/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.packaging.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class LintianResultParser {

    private static final Logger logger = LogManager.getLogger(LintianResultParser.class);
    private static final Pattern RESULT_PATTERN = Pattern.compile("(?<severity>[EW]): (?<package>\\S+): (?<tag>\\S+) (?<message>.+)");

    public Result parse(String output) {
        String[] lines = output.split("\n");
        List<Issue> issues = Arrays.stream(lines).map(line -> {
            Matcher matcher = RESULT_PATTERN.matcher(line);
            if (matcher.matches() == false) {
                logger.info("Lintian output not matching expected pattern: {}", line);
                return null;
            }
            Severity severity;
            switch (matcher.group("severity")) {
                case "E":
                    severity = Severity.ERROR;
                    break;
                case "W":
                    severity = Severity.WARNING;
                    break;
                default:
                    severity = Severity.UNKNOWN;
                    break;
            }
            return new Issue(severity, matcher.group("tag"), matcher.group("message"));
        }).filter(Objects::nonNull).collect(Collectors.toList());

        return new Result(issues.stream().noneMatch(it -> it.severity == Severity.ERROR || it.severity == Severity.WARNING), issues);
    }

    public static final class Result{
        private final boolean isSuccess;
        private final List<Issue> issues;

        public Result(boolean isSuccess, List<Issue> issues) {
            this.isSuccess = isSuccess;
            this.issues = issues;
        }

        public boolean isSuccess() {
            return isSuccess;
        }

        public List<Issue> issues() {
            return issues;
        }

        @Override
        public String toString() {
            return "Result{" +
                "isSuccess=" + isSuccess +
                ", issues=" + issues +
                '}';
        }
    }

    public static final class Issue {
        private final Severity severity;
        private final String tag;
        private final String message;

        public Issue(Severity severity, String tag, String message) {
            this.severity = severity;
            this.tag = tag;
            this.message = message;
        }

        public Severity severity() {
            return severity;
        }

        public String tag() {
            return tag;
        }

        public String message() {
            return message;
        }

        @Override
        public String toString() {
            return "Issue{" +
                "severity=" + severity +
                ", tag='" + tag + '\'' +
                ", message='" + message + '\'' +
                '}';
        }
    }

    public enum Severity {
        ERROR,
        WARNING,
        UNKNOWN
    }
}
