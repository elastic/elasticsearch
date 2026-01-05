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

class TransportVersionGenerationFuncTest extends AbstractTransportVersionFuncTest {

    def runGenerateAndValidateTask(String... additionalArgs) {
        List<String> args = new ArrayList<>()
        args.add(":myserver:validateTransportVersionResources")
        args.add(":myserver:generateTransportVersion")
        args.addAll(additionalArgs)
        return gradleRunner(args.toArray())
    }

    def runGenerateTask(String... additionalArgs) {
        List<String> args = new ArrayList<>()
        args.add(":myserver:generateTransportVersion")
        args.addAll(additionalArgs)
        return gradleRunner(args.toArray())
    }

    def runValidateTask() {
        return gradleRunner(":myserver:validateTransportVersionResources")
    }

    void assertGenerateSuccess(BuildResult result) {
        assert result.task(":myserver:generateTransportVersion").outcome == TaskOutcome.SUCCESS
    }

    void assertGenerateFailure(BuildResult result, String expectedOutput) {
        assert result.task(":myserver:generateTransportVersion").outcome == TaskOutcome.FAILED
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
        def result = runGenerateAndValidateTask().build()

        then:
        assertGenerateAndValidateSuccess(result)
    }

    def "a definition should be generated when specified by an arg but no code reference exists yet"() {
        when:
        def result = runGenerateTask("--name=new_tv").build()

        then:
        assertGenerateSuccess(result)
        assertReferableDefinition("new_tv", "8124000")
        assertUpperBound("9.2", "new_tv,8124000")

        when:
        referencedTransportVersion("new_tv")
        def validateResult = runValidateTask().build()

        then:
        assertValidateSuccess(validateResult)
    }

    def "a definition should be generated when only a code reference exists"() {
        given:
        referencedTransportVersion("new_tv")

        when:
        def result = runGenerateAndValidateTask().build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertReferableDefinition("new_tv", "8124000")
        assertUpperBound("9.2", "new_tv,8124000")
    }

    def "invalid changes to a upper bounds should be reverted"() {
        given:
        transportVersionUpperBound("9.2", "modification", "9000000")

        when:
        def result = runGenerateAndValidateTask().build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertUpperBound("9.2", "existing_92,8123000")
    }

    def "invalid changes to multiple upper bounds should be reverted"() {
        given:
        transportVersionUpperBound("9.2", "modification", "9000000")
        transportVersionUpperBound("9.1", "modification", "9000000")

        when:
        def result = runGenerateAndValidateTask("--backport-branches=9.1").build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertUpperBound("9.2", "existing_92,8123000")
        assertUpperBound("9.1", "existing_92,8012001")
    }

    def "unreferenced referable definition should be reverted"() {
        given:
        referableTransportVersion("test_tv", "8124000")
        transportVersionUpperBound("9.2", "test_tv", "8124000")

        when:
        def result = runGenerateAndValidateTask().build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertReferableDefinitionDoesNotExist("test_tv")
        assertUpperBound("9.2", "existing_92,8123000")
    }

    def "unreferenced referable definition in multiple branches should be reverted"() {
        given:
        referableTransportVersion("test_tv", "8124000")
        transportVersionUpperBound("9.2", "test_tv", "8124000")
        transportVersionUpperBound("9.1", "test_tv", "8012002")

        when:
        def result = runGenerateAndValidateTask().build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertReferableDefinitionDoesNotExist("test_tv")
        assertUpperBound("9.2", "existing_92,8123000")
        assertUpperBound("9.1", "existing_92,8012001")
    }

    def "a reference can be renamed"() {
        given:
        referableTransportVersion("first_tv", "8124000")
        transportVersionUpperBound("9.2", "first_tv", "8124000")
        referencedTransportVersion("renamed_tv")

        when:
        def result = runGenerateAndValidateTask().build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertReferableDefinitionDoesNotExist("first_tv")
        assertReferableDefinition("renamed_tv", "8124000")
        assertUpperBound("9.2", "renamed_tv,8124000")
    }

    def "a reference with a patch version can be renamed"() {
        given:
        referableTransportVersion("first_tv", "8124000,8012002")
        transportVersionUpperBound("9.2", "first_tv", "8124000")
        transportVersionUpperBound("9.1", "first_tv", "8012002")
        referencedTransportVersion("renamed_tv")

        when:
        def result = runGenerateAndValidateTask("--backport-branches=9.1").build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertReferableDefinitionDoesNotExist("first_tv")
        assertReferableDefinition("renamed_tv", "8124000,8012002")
        assertUpperBound("9.2", "renamed_tv,8124000")
        assertUpperBound("9.1", "renamed_tv,8012002")
    }

    def "a missing definition will be regenerated"() {
        given:
        referencedTransportVersion("test_tv")
        transportVersionUpperBound("9.2", "test_tv", "8124000")

        when:
        def result = runGenerateAndValidateTask().build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertReferableDefinition("test_tv", "8124000")
        assertUpperBound("9.2", "test_tv,8124000")
    }

    def "an upper bound can be regenerated"() {
        given:
        file("myserver/src/main/resources/transport/upper_bounds/9.2.csv").delete()
        referableAndReferencedTransportVersion("test_tv", "8124000")

        when:
        def result = runGenerateAndValidateTask().build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertUpperBound("9.2", "test_tv,8124000")
    }

    def "branches for definition can be removed"() {
        given:
        // previously generated with 9.1 and 9.2
        referableAndReferencedTransportVersion("test_tv", "8124000,8012002")
        transportVersionUpperBound("9.2", "test_tv", "8124000")
        transportVersionUpperBound("9.1", "test_tv", "8012002")

        when:
        def result = runGenerateAndValidateTask().build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertReferableDefinition("test_tv", "8124000")
        assertUpperBound("9.2", "test_tv,8124000")
        assertUpperBound("9.1", "existing_92,8012001")
    }

    def "branches for definition can be added"() {
        given:
        referableAndReferencedTransportVersion("test_tv", "8124000")
        transportVersionUpperBound("9.2", "test_tv", "8124000")

        when:
        def result = runGenerateAndValidateTask("--backport-branches=9.1").build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertReferableDefinition("test_tv", "8124000,8012002")
        assertUpperBound("9.2", "test_tv,8124000")
        assertUpperBound("9.1", "test_tv,8012002")
    }

    def "unreferenced definitions are removed"() {
        given:
        referableTransportVersion("test_tv", "8124000,8012002")
        transportVersionUpperBound("9.2", "test_tv", "8124000")
        transportVersionUpperBound("9.1", "test_tv", "8012002")

        when:
        def result = runGenerateAndValidateTask().build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertReferableDefinitionDoesNotExist("test_tv")
        assertUpperBound("9.2", "existing_92,8123000")
        assertUpperBound("9.1", "existing_92,8012001")
    }

    def "merge conflicts in latest files can be regenerated"() {
        given:
        file("myserver/src/main/resources/transport/latest/9.2.csv").text =
            """
            <<<<<<< HEAD
            existing_92,8123000
            =======
            second_tv,8123000
            >>>>>> name
            """.strip()
        referableAndReferencedTransportVersion("second_tv", "8123000")

        when:
        def result = runGenerateAndValidateTask("--name=second_tv", ).build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertReferableDefinition("existing_92", "8123000,8012001")
        assertReferableDefinition("second_tv", "8124000")
        assertUpperBound("9.2", "second_tv,8124000")
    }

    def "branches param order does not matter"() {
        given:
        referencedTransportVersion("test_tv")

        when:
        def result = runGenerateAndValidateTask("--backport-branches=9.0,9.1").build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertReferableDefinition("test_tv", "8124000,8012002,8000001")
        assertUpperBound("9.2", "test_tv,8124000")
        assertUpperBound("9.1", "test_tv,8012002")
        assertUpperBound("9.0", "test_tv,8000001")
    }

    def "if the files for a new definition already exist, no change should occur"() {
        given:
        transportVersionUpperBound("9.2", "test_tv", "8124000")
        referableAndReferencedTransportVersion("test_tv", "8124000")

        when:
        def result = runGenerateAndValidateTask().build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertReferableDefinition("test_tv", "8124000")
        assertUpperBound("9.2", "test_tv,8124000")
    }

    def "can add backport to latest upper bound"() {
        when:
        def result = runGenerateAndValidateTask("--name=existing_92", "--backport-branches=9.1,9.0").build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertReferableDefinition("existing_92", "8123000,8012001,8000001")
        assertUpperBound("9.2", "existing_92,8123000")
        assertUpperBound("9.1", "existing_92,8012001")
        assertUpperBound("9.0", "existing_92,8000001")
    }

    def "can add backport to older definition"() {
        given:
        execute("git checkout main")
        referableAndReferencedTransportVersion("latest_tv", "8124000,8012002")
        transportVersionUpperBound("9.2", "latest_tv", "8124000")
        transportVersionUpperBound("9.1", "latest_tv", "8012002")
        execute("git commit -a -m added")
        execute("git checkout -b mybranch")

        when:
        def result = runGenerateAndValidateTask("--name=existing_92", "--backport-branches=9.1,9.0").build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertReferableDefinition("existing_92", "8123000,8012001,8000001")
        assertUpperBound("9.2", "latest_tv,8124000")
        assertUpperBound("9.1", "latest_tv,8012002")
        assertUpperBound("9.0", "existing_92,8000001")
    }

    def "a different increment can be specified"() {
        given:
        referencedTransportVersion("new_tv")
        file("myserver/build.gradle") << """
            tasks.named('validateTransportVersionResources') {
                shouldValidateDensity = false
                shouldValidatePrimaryIdNotPatch = false
            }
        """

        when:
        def result = runGenerateAndValidateTask("--increment=100").build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertReferableDefinition("new_tv", "8123100")
        assertUpperBound("9.2", "new_tv,8123100")
    }

    def "a non-positive increment should fail"() {
        given:
        referencedTransportVersion("new_tv")

        when:
        def result = runGenerateTask("--increment=0").buildAndFail()

        then:
        assertOutputContains(result.output, "Invalid increment 0, must be a positive integer")
    }

    def "an increment larger than 1000 should fail"() {
        given:
        referencedTransportVersion("new_tv")

        when:
        def result = runGenerateTask("--increment=1001").buildAndFail()

        then:
        assertOutputContains(result.output, "Invalid increment 1001, must be no larger than 1000")
    }

    def "a new definition exists and is in the latest file, but the version id is wrong and needs to be updated"(){
        given:
        referableAndReferencedTransportVersion("new_tv", "1000000")
        transportVersionUpperBound("9.2", "new_tv", "1000000")

        when:
        def result = runGenerateAndValidateTask().build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertReferableDefinition("new_tv", "8124000")
        assertUpperBound("9.2", "new_tv,8124000")
    }

    def "backport branches is optional"() {
        given:
        referencedTransportVersion("new_tv")

        when:
        def result = runGenerateAndValidateTask().build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertReferableDefinition("new_tv", "8124000")
        assertUpperBound("9.2", "new_tv,8124000")
    }

    def "deleted upper bounds files are restored"() {
        given:
        file("myserver/src/main/resources/transport/upper_bounds/9.2.csv").delete()

        when:
        def result = runGenerateAndValidateTask().build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertUpperBound("9.2", "existing_92,8123000")
    }

    def "upper bounds files must exist for backport branches"() {
        when:
        def result = runGenerateTask("--name", "new_tv", "--backport-branches=9.1,8.13,7.17,6.0").buildAndFail()

        then:
        assertGenerateFailure(result, "Missing upper bounds files for branches [6.0, 7.17, 8.13], known branches are [8.19, 9.0, 9.1, 9.2]")
    }

    def "name can be found from committed definition"() {
        given:
        referableAndReferencedTransportVersion("new_tv", "8123000")
        execute("git add .")
        execute("git commit -m added")

        when:
        def result = runGenerateAndValidateTask().build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertUpperBound("9.2", "new_tv,8124000")
        assertReferableDefinition("new_tv", "8124000")
    }

    def "name can be found from staged definition"() {
        given:
        referableAndReferencedTransportVersion("new_tv", "8123000")
        execute("git add .")

        when:
        def result = runGenerateAndValidateTask().build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertUpperBound("9.2", "new_tv,8124000")
        assertReferableDefinition("new_tv", "8124000")
    }

    def "alternate upper bound larger"() {
        given:
        referencedTransportVersion("new_tv")
        file("myserver/alt_upper_bound.csv").text = "some_tv,8126000"
        file("myserver/build.gradle") << """
            tasks.named('generateTransportVersion') {
                alternateUpperBoundFile = project.file("alt_upper_bound.csv")
            }
            tasks.named('validateTransportVersionResources') {
                shouldValidateDensity = false
            }
        """

        when:
        def result = runGenerateAndValidateTask().build()
        then:
        assertGenerateAndValidateSuccess(result)
        assertUpperBound("9.2", "new_tv,8127000")
        assertReferableDefinition("new_tv", "8127000")
    }

    def "alternate upper bound less"() {
        given:
        referencedTransportVersion("new_tv")
        file("myserver/alt_upper_bound.csv").text = "some_tv,8122100"
        file("myserver/build.gradle") << """
            tasks.named('generateTransportVersion') {
                alternateUpperBoundFile = project.file("alt_upper_bound.csv")
            }
            tasks.named('validateTransportVersionResources') {
                shouldValidateDensity = false
            }
        """

        when:
        def result = runGenerateAndValidateTask().build()
        then:
        assertGenerateAndValidateSuccess(result)
        assertUpperBound("9.2", "new_tv,8124000")
        assertReferableDefinition("new_tv", "8124000")
    }

    def "generation is idempotent on upstream changes"() {
        given:
        execute("git checkout main")
        referableAndReferencedTransportVersion("new_tv", "8124000")
        transportVersionUpperBound("9.2", "new_tv", "8124000")
        execute("git add .")
        execute("git commit -m update")
        execute("git checkout test")

        when:
        def result = runGenerateAndValidateTask().build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertUpperBound("9.2", "existing_92,8123000")
    }

    def "generation cannot run on release branch"() {
        given:
        file("myserver/build.gradle") << """
            tasks.named('generateTransportVersion') {
                currentUpperBoundName = '9.1'
            }
        """

        when:
        def result = runGenerateTask().buildAndFail()

        then:
        assertGenerateFailure(result, "Transport version generation cannot run on release branches")
    }
}
