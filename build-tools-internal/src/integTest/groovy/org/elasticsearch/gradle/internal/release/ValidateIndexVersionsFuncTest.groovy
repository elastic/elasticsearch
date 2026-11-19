/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.release

import org.elasticsearch.gradle.internal.transport.AbstractTransportVersionFuncTest

/**
 * Covers the wiring the unit tests cannot: precommit runs the task, and the PR's target branch decides whether it reads main from git.
 * The test repository has no remote on GitHub's elastic organization, so reading main fails with a known error. Changelog validation
 * is skipped since it needs a schema file this build does not have.
 */
class ValidateIndexVersionsFuncTest extends AbstractTransportVersionFuncTest {

    def setup() {
        buildFile << """
            apply plugin: 'elasticsearch.release-tools'
        """
        file("server/src/main/java/org/elasticsearch/index/IndexVersions.java") << """
            package org.elasticsearch.index;
            class IndexVersions {
                static final IndexVersion FIRST = def(9_107_0_00, Version.LUCENE_10_5_0);
            }
        """
        file("server/src/main/resources/org/elasticsearch/index/IndexVersions.csv") << "9.1.0,9033000\n"
    }

    def "precommit reads main from git when a PR targets a release branch"() {
        when:
        def result = gradleRunnerWithEnv(Map.of("BUILDKITE_PULL_REQUEST_BASE_BRANCH", "9.6"), ":precommit", "-x", ":validateChangelogs").buildAndFail()

        then:
        result.output.contains("No git remote points to an elastic GitHub repository")
    }

    def "builds that are not PRs do not read git"() {
        when:
        def result = gradleRunner(":precommit", "-x", ":validateChangelogs").build()

        then:
        result.task(":validateIndexVersions").outcome.toString() == "SUCCESS"
    }
}
