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
import org.gradle.testkit.runner.TaskOutcome

class GenerateInitialTransportVersionFuncTest extends AbstractTransportVersionFuncTest {
    def runGenerateAndValidateTask(String... additionalArgs) {
        List<String> args = new ArrayList<>()
        args.add(":myserver:validateTransportVersionResources")
        args.add(":myserver:generateInitialTransportVersion")
        args.addAll(additionalArgs);
        return gradleRunner(args.toArray())
    }

    def runGenerateTask(String... additionalArgs) {
        List<String> args = new ArrayList<>()
        args.add(":myserver:generateInitialTransportVersion")
        args.addAll(additionalArgs);
        return gradleRunner(args.toArray())
    }

    void assertGenerateSuccess(BuildResult result) {
        assert result.task(":myserver:generateInitialTransportVersion").outcome == TaskOutcome.SUCCESS
    }

    void assertGenerateFailure(BuildResult result, String expectedOutput) {
        assert result.task(":myserver:generateInitialTransportVersion").outcome == TaskOutcome.FAILED
        assertOutputContains(result.output, expectedOutput)
    }

    void assertValidateSuccess(BuildResult result) {
        assert result.task(":myserver:validateTransportVersionResources").outcome == TaskOutcome.SUCCESS
    }

    void assertGenerateAndValidateSuccess(BuildResult result) {
        assertGenerateSuccess(result)
        assertValidateSuccess(result)
    }

    def "setup is valid"() {
        when:
        def result = runGenerateAndValidateTask("--stack-version", "9.1.0").build()

        then:
        assertGenerateAndValidateSuccess(result)
        // should have been idempotent, nothing actually changed
        assertNoChanges();
    }

    def "new minor also creates next upper bound"() {
        given:
        // version properties will be updated by release automation before running initial version generation
        versionPropertiesFile.text = versionPropertiesFile.text.replace("9.2.0", "9.3.0")

        when:
        def result = runGenerateAndValidateTask("--stack-version", "9.2.0").build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertUnreferableDefinition("initial_9.2.0", "8124000")
        assertUpperBound("9.2", "initial_9.2.0,8124000")
        assertUpperBound("9.3", "initial_9.2.0,8124000")
    }

    def "patch updates existing upper bound"() {
        when:
        def result = runGenerateAndValidateTask("--stack-version", "9.1.2").build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertUnreferableDefinition("initial_9.1.2", "8012002")
        assertUpperBound("9.1", "initial_9.1.2,8012002")
        // the current upper bound is in a different base, so the patch id does not affect it
        assertUpperBound("9.2", "existing_92,8123000")
    }

    def "patch also updates current upper bound in the same base"() {
        given: "a minor was branched, moving the current upper bound to that minor's initial version"
        execute("git checkout main")
        // version properties will be updated by release automation before running initial version generation
        versionPropertiesFile.text = versionPropertiesFile.text.replace("9.2.0", "9.3.0")
        assertGenerateSuccess(runGenerateTask("--stack-version", "9.2.0").build())
        execute("git add .")
        execute('git commit -m Feature-freeze-9.2')

        when: "the minor is finalized, generating the initial transport version for its first patch release"
        def result = runGenerateAndValidateTask("--stack-version", "9.2.1").build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertUnreferableDefinition("initial_9.2.1", "8124001")
        assertUpperBound("9.2", "initial_9.2.1,8124001")
        // no new transport version has been added since the branch was cut, so the current upper bound still points at
        // initial_9.2.0 in the same base, and must move forward with the patch id to remain the latest for that base
        assertUpperBound("9.3", "initial_9.2.1,8124001")
    }

    def "cannot create upper bound file for patch"() {
        when:
        def result = runGenerateTask("--stack-version", "9.3.7").buildAndFail()

        then:
        assertGenerateFailure(result, "Missing upper bound 9.3 for release version 9.3.7")
    }
}
