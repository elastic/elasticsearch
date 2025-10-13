/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport

import org.gradle.testkit.runner.BuildResult
import org.gradle.testkit.runner.GradleRunner
import org.gradle.testkit.runner.TaskOutcome

class ResolveTransportVersionConflictFuncTest extends AbstractTransportVersionFuncTest {

    GradleRunner runResolveAndValidateTask() {
        List<String> args = List.of(":myserver:validateTransportVersionResources", ":myserver:resolveTransportVersionConflict")
        return gradleRunner(args.toArray())
    }

    void assertResolveAndValidateSuccess(BuildResult result) {
        assert result.task(":myserver:resolveTransportVersionConflict").outcome == TaskOutcome.SUCCESS
        assert result.task(":myserver:validateTransportVersionResources").outcome == TaskOutcome.SUCCESS
    }

    def "update flag works with current"() {
        given:
        referableAndReferencedTransportVersion("new_tv", "8123000")
        file("myserver/src/main/resources/transport/latest/9.2.csv").text =
            """
            <<<<<<< HEAD
            existing_92,8123000
            =======
            new_tv,8123000
            >>>>>> name
            """.strip()

        when:
        def result = runResolveAndValidateTask().build()

        then:
        assertResolveAndValidateSuccess(result)
        assertReferableDefinition("existing_92", "8123000,8012001")
        assertReferableDefinition("new_tv", "8124000")
        assertUpperBound("9.2", "new_tv,8124000")
    }

    def "update flag works with multiple branches"() {
        given:
        referableAndReferencedTransportVersion("new_tv", "8123000,8012001,7123001")
        file("myserver/src/main/resources/transport/latest/9.2.csv").text =
            """
            <<<<<<< HEAD
            existing_92,8123000
            =======
            new_tv,8123000
            >>>>>> name
            """.strip()
        file("myserver/src/main/resources/transport/latest/9.1.csv").text =
            """
            <<<<<<< HEAD
            existing_92,8012001
            =======
            new_tv,8012001
            >>>>>> name
            """.strip()
        file("myserver/src/main/resources/transport/latest/8.19.csv").text =
            """
            <<<<<<< HEAD
            initial_8.19.7,7123001
            =======
            new_tv,7123001
            >>>>>> name
            """.strip()

        when:
        def result = runResolveAndValidateTask().build()

        then:
        assertResolveAndValidateSuccess(result)
        assertReferableDefinition("existing_92", "8123000,8012001")
        assertUnreferableDefinition("initial_8.19.7", "7123001")
        assertReferableDefinition("new_tv", "8124000,8012002,7123002")
        assertUpperBound("9.2", "new_tv,8124000")
        assertUpperBound("9.1", "new_tv,8012002")
        assertUpperBound("8.19", "new_tv,7123002")
    }

    def "no new transport version is idempotent"() {
        when:
        def result = runResolveAndValidateTask().build()

        then:
        assertResolveAndValidateSuccess(result)
        assertUpperBound("9.2", "existing_92,8123000")
    }

    def "upstream changes don't affect merge"() {
        given:
        // setup main with 2 commits, but we will only merge in the first one
        execute("git checkout main")
        referableAndReferencedTransportVersion("upstream_new_tv1", "8124000")
        transportVersionUpperBound("9.2", "upstream_new_tv1", "8124000")
        execute("git add .")
        execute("git commit -m update1")
        String toMerge = execute("git rev-parse HEAD")
        referableAndReferencedTransportVersion("upstream_new_tv2", "8125000")
        transportVersionUpperBound("9.2", "upstream_new_tv2", "8125000")
        execute("git add .")
        execute("git commit -m update2")
        execute("git checkout test")
        // now commit a conflict on the test branch, a new TV
        referableAndReferencedTransportVersion("branch_new_tv", "8124000")
        transportVersionUpperBound("9.2", "branch_new_tv", "8124000")
        execute("git add .")
        execute("git commit -m branch")
        // and finally initiate the merge
        System.out.println("Merging commit " + toMerge);
        execute("git merge " + toMerge, testProjectDir.root, true);

        when:
        def result = runResolveAndValidateTask().build()

        then:
        assertResolveAndValidateSuccess(result)
        assertUpperBound("9.2", "branch_new_tv,8125000")
    }
}
