/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport


import org.gradle.testkit.runner.TaskOutcome

class TransportVersionValidationFuncTest extends AbstractTransportVersionFuncTest {

    def "test setup works"() {
        when:
        def result = gradleRunner("validateTransportVersionResources", "validateTransportVersionReferences").build()
        then:
        result.task(":myserver:validateTransportVersionResources").outcome == TaskOutcome.SUCCESS
        result.task(":myserver:validateTransportVersionReferences").outcome == TaskOutcome.SUCCESS
        result.task(":myplugin:validateTransportVersionReferences").outcome == TaskOutcome.SUCCESS
    }

    def "definitions must be referenced"() {
        given:
        javaSource("myplugin", "org.elasticsearch.plugin", "MyPlugin",
            "import org.elasticsearch.TransportVersion;", """
            static final TransportVersion dne = TransportVersion.fromName("dne");
        """)
        when:
        def result = validateReferencesFails("myplugin")
        then:
        assertValidateReferencesFailure(result, "myplugin", "TransportVersion.fromName(\"dne\") was used at " +
                "org.elasticsearch.plugin.MyPlugin line 6, but lacks a transport version definition.")
    }

    def "references must be defined"() {
        given:
        referableTransportVersion("not_used", "1000000")
        when:
        def result = validateResourcesFails()
        then:
        assertValidateResourcesFailure(result, "Transport version definition file " +
            "[myserver/src/main/resources/transport/definitions/referable/not_used.csv] is not referenced")
    }

    def "names must be lowercase alphanum or underscore"() {
        given:
        referableAndReferencedTransportVersion("${name}", "8100000", "TestNames")
        when:
        def result = validateResourcesFails()
        then:
        assertValidateResourcesFailure(result, "Transport version definition file " +
            "[myserver/src/main/resources/transport/definitions/referable/${name}.csv] does not have a valid name, " +
            "must be lowercase alphanumeric and underscore")

        where:
        name << ["CapitalTV", "spaces tv", "trailing_spaces_tv ", "hyphen-tv", "period.tv"]
    }

    def "definitions contain at least one id"() {
        given:
        referableAndReferencedTransportVersion("empty", "")
        when:
        def result = validateResourcesFails()
        then:
        assertValidateResourcesFailure(result, "Transport version definition file " +
            "[myserver/src/main/resources/transport/definitions/referable/empty.csv] does not contain any ids")
    }

    def "definitions have ids in descending order"() {
        given:
        referableAndReferencedTransportVersion("out_of_order", "8100000,8200000")
        when:
        def result = validateResourcesFails()
        then:
        assertValidateResourcesFailure(result, "Transport version definition file " +
            "[myserver/src/main/resources/transport/definitions/referable/out_of_order.csv] does not have ordered ids")
    }

    def "definition ids are unique"() {
        given:
        referableAndReferencedTransportVersion("duplicate", "8123000")
        when:
        def result = validateResourcesFails()
        then:
        assertValidateResourcesFailure(result, "Transport version definition file " +
            "[myserver/src/main/resources/transport/definitions/referable/existing_92.csv] contains id 8123000 already defined in " +
            "[myserver/src/main/resources/transport/definitions/referable/duplicate.csv]")
    }

    def "definitions have bwc ids with non-zero patch part"() {
        given:
        referableAndReferencedTransportVersion("patched", "8200000,8100000")
        when:
        def result = validateResourcesFails()
        then:
        assertValidateResourcesFailure(result, "Transport version definition file " +
            "[myserver/src/main/resources/transport/definitions/referable/patched.csv] contains bwc id [8100000] with a patch part of 0")
    }

    def "definitions have primary ids which cannot change"() {
        given:
        referableTransportVersion("existing_92", "8500000")
        when:
        def result = validateResourcesFails()
        then:
        assertValidateResourcesFailure(result, "Transport version definition file " +
            "[myserver/src/main/resources/transport/definitions/referable/existing_92.csv] has modified primary id from 8123000 to 8500000")
    }

    def "cannot change committed ids to a branch"() {
        given:
        referableTransportVersion("existing_92", "8123000,8012002")
        when:
        def result = validateResourcesFails()
        then:
        assertValidateResourcesFailure(result, "Transport version definition file " +
            "[myserver/src/main/resources/transport/definitions/referable/existing_92.csv] has modified patch id from 8012001 to 8012002")
    }

    def "cannot change committed ids"() {
        given:
        referableTransportVersion("existing_92", "8123000")
        when:
        def result = validateResourcesFails()
        then:
        assertValidateResourcesFailure(result, "Transport version definition file " +
            "[myserver/src/main/resources/transport/definitions/referable/existing_92.csv] has removed id 8012001")
    }

    def "upper bounds files must reference defined name"() {
        given:
        transportVersionUpperBound("9.2", "dne", "8123000")
        when:
        def result = validateResourcesFails()
        then:
        assertValidateResourcesFailure(result, "Transport version upper bound file " +
            "[myserver/src/main/resources/transport/upper_bounds/9.2.csv] contains transport version name [dne] which is not defined")
    }

    def "upper bound files id must exist in definition"() {
        given:
        transportVersionUpperBound("9.2", "existing_92", "8124000")
        when:
        def result = validateResourcesFails()
        then:
        assertValidateResourcesFailure(result, "Transport version upper bound file " +
            "[myserver/src/main/resources/transport/upper_bounds/9.2.csv] has id 8124000 which is not in definition " +
            "[myserver/src/main/resources/transport/definitions/referable/existing_92.csv]")
    }

    def "upper bound files have latest id within base"() {
        given:
        transportVersionUpperBound("9.0", "seemingly_latest", "8110001")
        referableAndReferencedTransportVersion("original", "8110000")
        referableAndReferencedTransportVersion("seemingly_latest", "8111000,8110001")
        referableAndReferencedTransportVersion("actual_latest", "8112000,8110002")
        when:
        def result = validateResourcesFails()
        then:
        assertValidateResourcesFailure(result, "Transport version upper bound file " +
            "[myserver/src/main/resources/transport/upper_bounds/9.0.csv] has id 8110001 from [seemingly_latest] with base 8110000 " +
            "but another id 8110002 from [actual_latest] is later for that base")
    }

    def "upper bound files cannot change base id"() {
        given:
        referableAndReferencedTransportVersion("original", "8013000")
        referableAndReferencedTransportVersion("patch", "8015000,8013001")
        transportVersionUpperBound("9.1", "patch", "8013001")
        when:
        def result = validateResourcesFails()
        then:
        assertValidateResourcesFailure(result, "Transport version upper bound file " +
            "[myserver/src/main/resources/transport/upper_bounds/9.1.csv] modifies base id from 8012000 to 8013000")
    }

    def "ids must be dense"() {
        given:
        referableAndReferencedTransportVersion("original", "8013000")
        referableAndReferencedTransportVersion("patch1", "8015000,8013002")
        transportVersionUpperBound("9.0", "patch1", "8013002")
        when:
        def result = validateResourcesFails()
        then:
        assertValidateResourcesFailure(result, "Transport version base id 8013000 is missing patch ids between 8013000 and 8013002")
    }

    def "primary id must not be patch version"() {
        given:
        referableAndReferencedTransportVersion("patch", "8015001")
        when:
        def result = validateResourcesFails()
        then:
        assertValidateResourcesFailure(result, "Transport version definition file " +
            "[myserver/src/main/resources/transport/definitions/referable/patch.csv] has patch version 8015001 as primary id")
    }

    def "unreferable directory is optional"() {
        given:
        file("myserver/src/main/resources/transport/unreferable/initial_9_0_0.csv").delete()
        file("myserver/src/main/resources/transport/unreferable").deleteDir()
        when:
        def result = gradleRunner(":myserver:validateTransportVersionResources").build()
        then:
        result.task(":myserver:validateTransportVersionResources").outcome == TaskOutcome.SUCCESS
    }

    def "upper bound can refer to an unreferable definition"() {
        given:
        unreferableTransportVersion("initial_9.3.0", "8124000")
        transportVersionUpperBound("9.3", "initial_9.3.0", "8124000")
        when:
        def result = gradleRunner(":myserver:validateTransportVersionResources").build()
        then:
        result.task(":myserver:validateTransportVersionResources").outcome == TaskOutcome.SUCCESS
    }

    def "referable and unreferable definitions cannot have the same name"() {
        given:
        unreferableTransportVersion("existing_92", "10000000")
        when:
        def result = validateResourcesFails()
        then:
        assertValidateResourcesFailure(result, "Transport version definition file " +
                "[myserver/src/main/resources/transport/definitions/referable/existing_92.csv] " +
                "has same name as unreferable definition " +
                "[myserver/src/main/resources/transport/definitions/unreferable/existing_92.csv]")
    }

    def "unreferable definitions can have primary ids that are patches"() {
        given:
        unreferableTransportVersion("initial_7.0.1", "7000001")
        when:
        def result = gradleRunner(":myserver:validateTransportVersionResources").build()
        then:
        result.task(":myserver:validateTransportVersionResources").outcome == TaskOutcome.SUCCESS
    }

    def "highest id in an referable definition should exist in an upper bounds file"() {
        given:
        referableAndReferencedTransportVersion("some_tv", "8124000")
        when:
        def result = validateResourcesFails()
        then:
        assertValidateResourcesFailure(result, "Transport version definition file " +
            "[myserver/src/main/resources/transport/definitions/referable/some_tv.csv] " +
            "has the highest transport version id [8124000] but is not present in any upper bounds files")
    }

    def "highest id in an unreferable definition should exist in an upper bounds file"() {
        given:
        unreferableTransportVersion("initial_9.3.0", "8124000")
        when:
        def result = validateResourcesFails()
        then:
        assertValidateResourcesFailure(result, "Transport version definition file " +
            "[myserver/src/main/resources/transport/definitions/unreferable/initial_9.3.0.csv] " +
            "has the highest transport version id [8124000] but is not present in any upper bounds files")
    }

    def "primary ids cannot jump ahead too fast"() {
        given:
        referableAndReferencedTransportVersion("some_tv", "8125000")
        transportVersionUpperBound("9.2", "some_tv", "8125000")

        when:
        def result = validateResourcesFails()

        then:
        assertValidateResourcesFailure(result, "Transport version definition file " +
            "[myserver/src/main/resources/transport/definitions/referable/some_tv.csv] " +
            "has primary id 8125000 which is more than maximum increment 1000 from id 8123000 in definition " +
            "[myserver/src/main/resources/transport/definitions/referable/existing_92.csv]"
        )
    }

    def "primary id checks skipped on release branch"() {
        given:
        file("myserver/build.gradle") << """
            tasks.named('validateTransportVersionResources') {
                currentUpperBoundName = '9.1'
            }
        """
        referableAndReferencedTransportVersion("some_tv", "8125000")
        transportVersionUpperBound("9.2", "some_tv", "8125000")

        when:
        def result = gradleRunner("validateTransportVersionResources").build()

        then:
        result.task(":myserver:validateTransportVersionResources").outcome == TaskOutcome.SUCCESS
    }

    def "only current upper bound validated on release branch"() {
        given:
        file("myserver/build.gradle") << """
            tasks.named('validateTransportVersionResources') {
                currentUpperBoundName = '9.0'
            }
        """
        referableAndReferencedTransportVersion("some_tv", "8124000,8012004")
        transportVersionUpperBound("9.1", "some_tv", "8012004")

        when:
        def result = gradleRunner("validateTransportVersionResources").build()

        then:
        result.task(":myserver:validateTransportVersionResources").outcome == TaskOutcome.SUCCESS
    }
}
