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
        args.add(":myserver:validateTransportVersionDefinitions")
        args.add(":myserver:generateTransportVersionDefinition")
        args.addAll(additionalArgs);
        return gradleRunner(args.toArray())
    }

    def runGenerateTask(String... additionalArgs) {
        List<String> args = new ArrayList<>()
        args.add(":myserver:generateTransportVersionDefinition")
        args.addAll(additionalArgs);
        return gradleRunner(args.toArray())
    }

    def runValidateTask() {
        return gradleRunner(":myserver:validateTransportVersionDefinitions")
    }

    void assertGenerateSuccess(BuildResult result) {
        assert result.task(":myserver:generateTransportVersionDefinition").outcome == TaskOutcome.SUCCESS
    }

    void assertGenerateFailure(BuildResult result) {
        assert result.task(":myserver:generateTransportVersionDefinition").outcome == TaskOutcome.FAILED
    }

    void assertValidateSuccess(BuildResult result) {
        assert result.task(":myserver:validateTransportVersionDefinitions").outcome == TaskOutcome.SUCCESS
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
        def result = runGenerateTask("--name=new_tv", "--branches=9.2").build()

        then:
        assertGenerateSuccess(result)
        assertNamedDefinition("new_tv", "8124000")
        assertLatest("9.2", "new_tv,8124000")

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
        def result = runGenerateAndValidateTask("--branches=9.2").build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertNamedDefinition("new_tv", "8124000")
        assertLatest("9.2", "new_tv,8124000")
    }

    def "generation fails if branches omitted outside CI"() {
        when:
        def generateResult = runGenerateTask("--name=no_branches").buildAndFail()
        def validateResult = runValidateTask().build()

        then:
        assertGenerateFailure(generateResult)
        assertOutputContains(generateResult.output, "When running outside CI, --branches must be specified")
        assertValidateSuccess(validateResult)
    }

    def "invalid changes to a latest file should be reverted"() {
        given:
        latestTransportVersion("9.2", "modification", "9000000")

        when:
        def result = runGenerateAndValidateTask("--branches=9.2").build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertLatest("9.2", "existing_92,8123000")
    }

    def "invalid changes to multiple latest files should be reverted"() {
        given:
        latestTransportVersion("9.2", "modification", "9000000")
        latestTransportVersion("9.1", "modification", "9000000")

        when:
        def result = runGenerateAndValidateTask("--branches=9.2,9.1").build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertLatest("9.2", "existing_92,8123000")
        assertLatest("9.1", "existing_92,8012001")
    }

    def "unreferenced referable definition should be reverted"() {
        given:
        namedTransportVersion("test_tv", "8124000")
        latestTransportVersion("9.2", "test_tv", "8124000")

        when:
        def result = runGenerateAndValidateTask().build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertNamedDefinitionDoesNotExist("test_tv")
        assertLatest("9.2", "existing_92,8123000")
    }

    def "unreferenced referable definition in multiple branches should be reverted"() {
        given:
        namedTransportVersion("test_tv", "8124000")
        latestTransportVersion("9.2", "test_tv", "8124000")
        latestTransportVersion("9.1", "test_tv", "8012002")

        when:
        def result = runGenerateAndValidateTask().build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertNamedDefinitionDoesNotExist("test_tv")
        assertLatest("9.2", "existing_92,8123000")
        assertLatest("9.1", "existing_92,8012001")
    }

    def "a reference can be renamed"() {
        given:
        namedTransportVersion("first_tv", "8124000")
        latestTransportVersion("9.2", "first_tv", "8124000")
        referencedTransportVersion("renamed_tv")

        when:
        def result = runGenerateAndValidateTask("--branches=9.2").build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertNamedDefinitionDoesNotExist("first_tv")
        assertNamedDefinition("renamed_tv", "8124000")
        assertLatest("9.2", "renamed_tv,8124000")
    }

    def "a reference with a patch version can be renamed"() {
        given:
        namedTransportVersion("first_tv", "8124000,8012002")
        latestTransportVersion("9.2", "first_tv", "8124000")
        latestTransportVersion("9.1", "first_tv", "8012002")
        referencedTransportVersion("renamed_tv")

        when:
        def result = runGenerateAndValidateTask("--branches=9.2,9.1").build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertNamedDefinitionDoesNotExist("first_tv")
        assertNamedDefinition("renamed_tv", "8124000,8012002")
        assertLatest("9.2", "renamed_tv,8124000")
        assertLatest("9.1", "renamed_tv,8012002")
    }

    def "a missing definition will be regenerated"() {
        given:
        referencedTransportVersion("test_tv")
        latestTransportVersion("9.2", "test_tv", "8124000")

        when:
        def result = runGenerateAndValidateTask("--branches=9.2").build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertNamedDefinition("test_tv", "8124000")
        assertLatest("9.2", "test_tv,8124000")
    }

    def "a latest file can be regenerated"() {
        given:
        definedAndUsedTransportVersion("test_tv", "8124000")

        when:
        def result = runGenerateAndValidateTask("--branches=9.2").build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertLatest("9.2", "test_tv,8124000")
    }

    def "branches for definition can be removed"() {
        given:
        // previously generated with 9.1 and 9.2
        definedAndUsedTransportVersion("test_tv", "8124000,8012002")
        latestTransportVersion("9.2", "test_tv", "8124000")
        latestTransportVersion("9.1", "test_tv", "8012002")

        when:
        def result = runGenerateAndValidateTask("--branches=9.2").build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertNamedDefinition("test_tv", "8124000")
        assertLatest("9.2", "test_tv,8124000")
        assertLatest("9.1", "existing_92,8012001")
    }

    def "branches for definition can be added"() {
        given:
        definedAndUsedTransportVersion("test_tv", "8124000")
        latestTransportVersion("9.2", "test_tv", "8124000")

        when:
        def result = runGenerateAndValidateTask("--branches=9.2,9.1").build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertNamedDefinition("test_tv", "8124000,8012002")
        assertLatest("9.2", "test_tv,8124000")
        assertLatest("9.1", "test_tv,8012002")
    }

    def "unreferenced definitions are removed"() {
        given:
        namedTransportVersion("test_tv", "8124000,8012002")
        latestTransportVersion("9.2", "test_tv", "8124000")
        latestTransportVersion("9.1", "test_tv", "8012002")

        when:
        def result = runGenerateAndValidateTask().build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertNamedDefinitionDoesNotExist("test_tv")
        assertLatest("9.2", "existing_92,8123000")
        assertLatest("9.1", "existing_92,8012001")
    }

    def "merge conflicts in latest files can be regenerated"() {
        given:
        file("myserver/src/main/resources/transport/latest/9.2.csv").text =
            """
            <<<<<<< HEAD
            existing_92,8123000
            =======
            second_tv,8123000
            >>>>>> branch
            """.strip()
        definedAndUsedTransportVersion("second_tv", "8123000")

        when:
        def result = runGenerateAndValidateTask("--name=second_tv", "--branches=9.2").build()

        then:
        assertGenerateAndValidateSuccess(result)
        assertNamedDefinition("existing_92", "8123000,8012001")
        assertNamedDefinition("second_tv", "8124000")
        assertLatest("9.2", "second_tv,8124000")
    }
}
