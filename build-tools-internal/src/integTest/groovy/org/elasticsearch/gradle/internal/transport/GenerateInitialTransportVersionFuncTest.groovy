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

    def "patch reserves a new base for the current branch in the same base"() {
        given:
        featureFreezeNewMinor()

        when: "the minor is finalized, generating the initial transport version for its first patch release"
        def result = runGenerateAndValidateTask("--stack-version", "9.2.1").build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertUnreferableDefinition("initial_9.2.1", "8124001")
        assertUpperBound("9.2", "initial_9.2.1,8124001")
        // no transport version has been added to the current branch since the minor was branched, so its upper bound
        // still points at initial_9.2.0 in the same base. The patch id cannot be adopted here, so a new base is
        // reserved for the current branch instead.
        assertUnreferableDefinition("placeholder_9.3.0", "8125000")
        assertUpperBound("9.3", "placeholder_9.3.0,8125000")
    }

    def "patch of the current minor does not reserve a base"() {
        when: "the release branch generates the initial version for its own next patch release"
        def result = runGenerateAndValidateTask("--stack-version", "9.2.1").build()

        then: "its upper bound is the patch id itself, and no base is reserved"
        assertGenerateAndValidateSuccess(result)
        assertUnreferableDefinition("initial_9.2.1", "8123001")
        assertUpperBound("9.2", "initial_9.2.1,8123001")
        file("myserver/src/main/resources/transport/definitions/unreferable/placeholder_9.2.0.csv").exists() == false
    }

    def "transport version generated after a reserved base gets a base id"() {
        given:
        featureFreezeNewMinor()
        assertGenerateSuccess(runGenerateTask("--stack-version", "9.2.1").build())
        execute("git add .")
        execute('git commit -m Finalize-9.2.0')

        when: "a transport version is added to the current branch"
        referencedTransportVersion("new_tv")
        def result = gradleRunner(
            ":myserver:validateTransportVersionResources",
            ":myserver:generateTransportVersion",
            "--name",
            "new_tv"
        ).build()

        then: "it increments from the reserved base, rather than from a patch id"
        result.task(":myserver:generateTransportVersion").outcome == TaskOutcome.SUCCESS
        assertValidateSuccess(result)
        assertReferableDefinition("new_tv", "8126000")
        assertUpperBound("9.3", "new_tv,8126000")
    }

    /**
     * Cuts a release branch for 9.2 the way release automation does, leaving the current branch at 9.3.0 with its upper
     * bound pointing at the initial version of the minor that was just branched.
     */
    private void featureFreezeNewMinor() {
        execute("git checkout main")
        // version properties will be updated by release automation before running initial version generation
        versionPropertiesFile.text = versionPropertiesFile.text.replace("9.2.0", "9.3.0")
        // the branch has moved on to 9.3.0, so that is the upper bound the transport version tasks work against
        file("myserver/build.gradle") << """
            tasks.named('generateTransportVersion') {
                currentUpperBoundName = '9.3'
            }
            tasks.named('validateTransportVersionResources') {
                currentUpperBoundName = '9.3'
            }
        """
        assertGenerateSuccess(runGenerateTask("--stack-version", "9.2.0").build())
        execute("git add .")
        execute('git commit -m Feature-freeze-9.2')
    }

    def "cannot create upper bound file for patch"() {
        when:
        def result = runGenerateTask("--stack-version", "9.3.7").buildAndFail()

        then:
        assertGenerateFailure(result, "Missing upper bound 9.3 for release version 9.3.7")
    }
}
