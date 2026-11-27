/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.conventions.problems;

import org.gradle.api.problems.ProblemId;
import org.gradle.api.problems.Problems;
import org.gradle.api.problems.ThirdLevelProblemGroup;

/**
 * Problem groups for Elasticsearch build problems reported through the Gradle Problems API. These problems show up in
 * the Tooling API and in the local problems report ({@code build/reports/problems/problems-report.html}).
 *
 * <p>Since Gradle 9.9, problem groups hang off the predefined hierarchy provided by {@link Problems#getGroups()}.
 * Our checks verify the build and its sources, so they are reported under the predefined {@code Verification} root,
 * in one of two Elasticsearch specific second-level groups ({@link Category}). Each constant is a third-level group
 * for one check, e.g. {@code Verification > Elasticsearch Precommit > Forbidden Patterns}.
 *
 * <p>Problem ids are named after the <em>kind</em> of problem (the name is also its display name). Put details
 * that vary per occurrence (file, class, rule, ...) in the contextual label instead of the problem name.
 */
public enum ElasticsearchBuildProblems {

    FORBIDDEN_PATTERNS(Category.PRECOMMIT, "Forbidden Patterns"),
    LICENSE_HEADERS(Category.PRECOMMIT, "License Headers"),
    TESTING_CONVENTIONS(Category.PRECOMMIT, "Testing Conventions"),
    FORBIDDEN_APIS(Category.PRECOMMIT, "Forbidden APIs"),
    MISSING_CLASSES(Category.PRECOMMIT, "Missing Classes"),
    JAR_HELL(Category.PRECOMMIT, "JDK Jar Hell"),
    DEPENDENCY_LICENSES(Category.PRECOMMIT, "Dependency Licenses"),
    SPLIT_PACKAGES(Category.PRECOMMIT, "Split Packages"),
    JAVA_MODULE(Category.PRECOMMIT, "Java Module"),
    POM_VALIDATION(Category.PRECOMMIT, "POM Validation"),
    JSON_VALIDATION(Category.PRECOMMIT, "JSON Validation"),

    BUILD_GRADLE_SCRIPTS(Category.BUILD_LOGIC, "Build Gradle Scripts");

    /**
     * Elasticsearch specific second-level groups below the predefined {@code Verification} root group.
     */
    public enum Category {
        PRECOMMIT("Elasticsearch Precommit"),
        BUILD_LOGIC("Elasticsearch Build Logic");

        private final String groupName;

        Category(String groupName) {
            this.groupName = groupName;
        }

        public String groupName() {
            return groupName;
        }
    }

    private final Category category;
    private final String groupName;

    ElasticsearchBuildProblems(Category category, String groupName) {
        this.category = category;
        this.groupName = groupName;
    }

    public Category category() {
        return category;
    }

    public String groupName() {
        return groupName;
    }

    /**
     * Resolves this group in the predefined problem group hierarchy of the given {@link Problems} service.
     */
    public ThirdLevelProblemGroup group(Problems problems) {
        return problems.getGroups().getVerification().group(category.groupName).group(groupName);
    }

    /**
     * Shorthand for {@code group(problems).problemId(name)}.
     */
    public ProblemId problemId(Problems problems, String name) {
        return group(problems).problemId(name);
    }
}
