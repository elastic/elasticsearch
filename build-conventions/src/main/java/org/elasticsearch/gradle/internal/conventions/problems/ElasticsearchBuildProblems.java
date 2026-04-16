/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.conventions.problems;

import org.gradle.api.problems.ProblemGroup;

/**
 * Centralized problem group hierarchy for Elasticsearch build problems.
 * These groups are used with the Gradle Problems API to provide structured,
 * categorized error reporting that integrates with the Tooling API and the
 * local problems report ({@code build/reports/problems/problems-report.html}).
 */
public final class ElasticsearchBuildProblems {

    private ElasticsearchBuildProblems() {}

    // Root group
    public static final ProblemGroup ROOT = ProblemGroup.create("elasticsearch-build", "Elasticsearch Build");

    // Second-level groups
    public static final ProblemGroup PRECOMMIT = ProblemGroup.create("precommit", "Precommit Checks", ROOT);

    // Leaf groups under PRECOMMIT
    public static final ProblemGroup FORBIDDEN_PATTERNS = ProblemGroup.create("forbidden-patterns", "Forbidden Patterns", PRECOMMIT);
    public static final ProblemGroup LICENSE_HEADERS = ProblemGroup.create("license-headers", "License Headers", PRECOMMIT);
    public static final ProblemGroup TESTING_CONVENTIONS = ProblemGroup.create(
        "testing-conventions",
        "Testing Conventions",
        PRECOMMIT
    );
    public static final ProblemGroup FORBIDDEN_APIS = ProblemGroup.create("forbidden-apis", "Forbidden APIs", PRECOMMIT);
    public static final ProblemGroup MISSING_CLASSES = ProblemGroup.create("missing-classes", "Missing Classes", PRECOMMIT);
    public static final ProblemGroup JAR_HELL = ProblemGroup.create("jar-hell", "JDK Jar Hell", PRECOMMIT);
    public static final ProblemGroup DEPENDENCY_LICENSES = ProblemGroup.create(
        "dependency-licenses",
        "Dependency Licenses",
        PRECOMMIT
    );
    public static final ProblemGroup SPLIT_PACKAGES = ProblemGroup.create("split-packages", "Split Packages", PRECOMMIT);
    public static final ProblemGroup JAVA_MODULE = ProblemGroup.create("java-module", "Java Module", PRECOMMIT);
    public static final ProblemGroup POM_VALIDATION = ProblemGroup.create("pom-validation", "POM Validation", PRECOMMIT);
    public static final ProblemGroup JSON_VALIDATION = ProblemGroup.create("json-validation", "JSON Validation", PRECOMMIT);
}
