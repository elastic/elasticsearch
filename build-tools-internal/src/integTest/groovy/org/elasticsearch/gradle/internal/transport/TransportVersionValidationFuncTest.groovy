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
        namedTransportVersion("not_used", "1000000")
        when:
        def result = validateResourcesFails()
        then:
        assertValidateResourcesFailure(result, "Transport version definition file " +
            "[myserver/src/main/resources/transport/definitions/named/not_used.csv] is not referenced")
    }

    def "names must be lowercase alphanum or underscore"() {
        given:
        namedAndReferencedTransportVersion("${name}", "8100000", "TestNames")
        when:
        def result = validateResourcesFails()
        then:
        assertValidateResourcesFailure(result, "Transport version definition file " +
            "[myserver/src/main/resources/transport/definitions/named/${name}.csv] does not have a valid name, " +
            "must be lowercase alphanumeric and underscore")

        where:
        name << ["CapitalTV", "spaces tv", "trailing_spaces_tv ", "hyphen-tv", "period.tv"]
    }

    def "definitions contain at least one id"() {
        given:
        namedAndReferencedTransportVersion("empty", "")
        when:
        def result = validateResourcesFails()
        then:
        assertValidateResourcesFailure(result, "Transport version definition file " +
            "[myserver/src/main/resources/transport/definitions/named/empty.csv] does not contain any ids")
    }

    def "definitions have ids in descending order"() {
        given:
        namedAndReferencedTransportVersion("out_of_order", "8100000,8200000")
        when:
        def result = validateResourcesFails()
        then:
        assertValidateResourcesFailure(result, "Transport version definition file " +
            "[myserver/src/main/resources/transport/definitions/named/out_of_order.csv] does not have ordered ids")
    }

    def "definition ids are unique"() {
        given:
        namedAndReferencedTransportVersion("duplicate", "8123000")
        when:
        def result = validateResourcesFails()
        then:
        assertValidateResourcesFailure(result, "Transport version definition file " +
            "[myserver/src/main/resources/transport/definitions/named/existing_92.csv] contains id 8123000 already defined in " +
            "[myserver/src/main/resources/transport/definitions/named/duplicate.csv]")
    }

    def "definitions have bwc ids with non-zero patch part"() {
        given:
        namedAndReferencedTransportVersion("patched", "8200000,8100000")
        when:
        def result = validateResourcesFails()
        then:
        assertValidateResourcesFailure(result, "Transport version definition file " +
            "[myserver/src/main/resources/transport/definitions/named/patched.csv] contains bwc id [8100000] with a patch part of 0")
    }

    def "definitions have primary ids which cannot change"() {
        given:
        namedTransportVersion("existing_92", "8500000")
        when:
        def result = validateResourcesFails()
        then:
        assertValidateResourcesFailure(result, "Transport version definition file " +
            "[myserver/src/main/resources/transport/definitions/named/existing_92.csv] has modified primary id from 8123000 to 8500000")
    }

    def "cannot change committed ids to a branch"() {
        given:
        namedTransportVersion("existing_92", "8123000,8012002")
        when:
        def result = validateResourcesFails()
        then:
        assertValidateResourcesFailure(result, "Transport version definition file " +
            "[myserver/src/main/resources/transport/definitions/named/existing_92.csv] modifies existing patch id from 8012001 to 8012002")
    }

    def "latest files must reference defined name"() {
        given:
        latestTransportVersion("9.2", "dne", "8123000")
        when:
        def result = validateResourcesFails()
        then:
        assertValidateResourcesFailure(result, "Latest transport version file " +
            "[myserver/src/main/resources/transport/latest/9.2.csv] contains transport version name [dne] which is not defined")
    }

    def "latest files id must exist in definition"() {
        given:
        latestTransportVersion("9.2", "existing_92", "8124000")
        when:
        def result = validateResourcesFails()
        then:
        assertValidateResourcesFailure(result, "Latest transport version file " +
            "[myserver/src/main/resources/transport/latest/9.2.csv] has id 8124000 which is not in definition " +
            "[myserver/src/main/resources/transport/definitions/named/existing_92.csv]")
    }

    def "latest files have latest id within base"() {
        given:
        latestTransportVersion("9.0", "seemingly_latest", "8110001")
        namedAndReferencedTransportVersion("original", "8110000")
        namedAndReferencedTransportVersion("seemingly_latest", "8111000,8110001")
        namedAndReferencedTransportVersion("actual_latest", "8112000,8110002")
        when:
        def result = validateResourcesFails()
        then:
        assertValidateResourcesFailure(result, "Latest transport version file " +
            "[myserver/src/main/resources/transport/latest/9.0.csv] has id 8110001 from [seemingly_latest] with base 8110000 " +
            "but another id 8110002 from [actual_latest] is later for that base")
    }

    def "latest files cannot change base id"() {
        given:
        namedAndReferencedTransportVersion("original", "8013000")
        namedAndReferencedTransportVersion("patch", "8015000,8013001")
        latestTransportVersion("9.1", "patch", "8013001")
        when:
        def result = validateResourcesFails()
        then:
        assertValidateResourcesFailure(result, "Latest transport version file " +
            "[myserver/src/main/resources/transport/latest/9.1.csv] modifies base id from 8012000 to 8013000")
    }

    def "ids must be dense"() {
        given:
        namedAndReferencedTransportVersion("original", "8013000")
        namedAndReferencedTransportVersion("patch1", "8015000,8013002")
        latestTransportVersion("9.0", "patch1", "8013002")
        when:
        def result = validateResourcesFails()
        then:
        assertValidateResourcesFailure(result, "Transport version base id 8013000 is missing patch ids between 8013000 and 8013002")
    }

    def "primary id must not be patch version"() {
        given:
        namedAndReferencedTransportVersion("patch", "8015001")
        when:
        def result = validateResourcesFails()
        then:
        assertValidateResourcesFailure(result, "Transport version definition file " +
            "[myserver/src/main/resources/transport/definitions/named/patch.csv] has patch version 8015001 as primary id")
    }

    def "unreferenced directory is optional"() {
        given:
        file("myserver/src/main/resources/transport/unreferenced/initial_9_0_0.csv").delete()
        file("myserver/src/main/resources/transport/unreferenced").deleteDir()
        when:
        def result = gradleRunner(":myserver:validateTransportVersionResources").build()
        then:
        result.task(":myserver:validateTransportVersionResources").outcome == TaskOutcome.SUCCESS
    }

    def "latest can refer to an unreferenced definition"() {
        given:
        unreferencedTransportVersion("initial_10.0.0", "10000000")
        latestTransportVersion("10.0", "initial_10.0.0", "10000000")
        when:
        def result = gradleRunner(":myserver:validateTransportVersionResources").build()
        then:
        result.task(":myserver:validateTransportVersionResources").outcome == TaskOutcome.SUCCESS
    }

    def "named and unreferenced definitions cannot have the same name"() {
        given:
        unreferencedTransportVersion("existing_92", "10000000")
        when:
        def result = validateResourcesFails()
        then:
        assertValidateResourcesFailure(result, "Transport version definition file " +
                "[myserver/src/main/resources/transport/definitions/named/existing_92.csv] " +
                "has same name as unreferenced definition " +
                "[myserver/src/main/resources/transport/definitions/unreferenced/existing_92.csv]")
    }

    def "unreferenced definitions can have primary ids that are patches"() {
        given:
        unreferencedTransportVersion("initial_10.0.1", "10000001")
        when:
        def result = gradleRunner(":myserver:validateTransportVersionResources").build()
        then:
        result.task(":myserver:validateTransportVersionResources").outcome == TaskOutcome.SUCCESS
    }
}
