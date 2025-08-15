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

class TransportVersionManagementPluginFuncTest extends AbstractTransportVersionFuncTest {
    def "test setup works"() {
        when:
        def result = gradleRunner("validateTransportVersionDefinitions", "validateTransportVersionReferences").build()
        then:
        result.task(":myserver:validateTransportVersionDefinitions").outcome == TaskOutcome.SUCCESS
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
        assertReferencesFailure(result, "myplugin", "TransportVersion.fromName(\"dne\") was used at " +
                "org.elasticsearch.plugin.MyPlugin line 6, but lacks a transport version definition.")
    }

    def "references must be defined"() {
        given:
        definedTransportVersion("not_used", "1000000")
        when:
        def result = validateDefinitionsFails()
        then:
        assertDefinitionsFailure(result, "Transport version definition file " +
            "[myserver/src/main/resources/transport/defined/not_used.csv] is not referenced")
    }

    def "names must be lowercase alphanum or underscore"() {
        given:
        definedAndUsedTransportVersion("${name}", "8100000", "TestNames")
        when:
        def result = validateDefinitionsFails()
        then:
        assertDefinitionsFailure(result, "Transport version definition file " +
            "[myserver/src/main/resources/transport/defined/${name}.csv] does not have a valid name, " +
            "must be lowercase alphanumeric and underscore")

        where:
        name << ["CapitalTV", "spaces tv", "trailing_spaces_tv ", "hyphen-tv", "period.tv"]
    }

    def "definitions contain at least one id"() {
        given:
        definedAndUsedTransportVersion("empty", "")
        when:
        def result = validateDefinitionsFails()
        then:
        assertDefinitionsFailure(result, "Transport version definition file " +
            "[myserver/src/main/resources/transport/defined/empty.csv] does not contain any ids")
    }

    def "definitions have ids in descending order"() {
        given:
        definedAndUsedTransportVersion("out_of_order", "8100000,8200000")
        when:
        def result = validateDefinitionsFails()
        then:
        assertDefinitionsFailure(result, "Transport version definition file " +
            "[myserver/src/main/resources/transport/defined/out_of_order.csv] does not have ordered ids")
    }

    def "definition ids are unique"() {
        given:
        definedAndUsedTransportVersion("duplicate", "8123000")
        when:
        def result = validateDefinitionsFails()
        then:
        assertDefinitionsFailure(result, "Transport version definition file " +
            "[myserver/src/main/resources/transport/defined/existing_92.csv] contains id 8123000 already defined in " +
            "[myserver/src/main/resources/transport/defined/duplicate.csv]")
    }

    def "definitions have bwc ids with non-zero patch part"() {
        given:
        definedAndUsedTransportVersion("patched", "8200000,8100000")
        when:
        def result = validateDefinitionsFails()
        then:
        assertDefinitionsFailure(result, "Transport version definition file " +
            "[myserver/src/main/resources/transport/defined/patched.csv] contains bwc id [8100000] with a patch part of 0")
    }

    def "definitions have primary ids which cannot change"() {
        given:
        definedTransportVersion("existing_92", "8500000")
        when:
        def result = validateDefinitionsFails()
        then:
        assertDefinitionsFailure(result, "Transport version definition file " +
            "[myserver/src/main/resources/transport/defined/existing_92.csv] has modified primary id from 8123000 to 8500000")
    }

    def "cannot change committed ids to a branch"() {
        given:
        definedTransportVersion("existing_92", "8123000,8012002")
        when:
        def result = validateDefinitionsFails()
        then:
        assertDefinitionsFailure(result, "Transport version definition file " +
            "[myserver/src/main/resources/transport/defined/existing_92.csv] modifies existing patch id from 8012001 to 8012002")
    }

    def "latest files must reference defined name"() {
        given:
        latestTransportVersion("9.2", "dne", "8123000")
        when:
        def result = validateDefinitionsFails()
        then:
        assertDefinitionsFailure(result, "Latest transport version file " +
            "[myserver/src/main/resources/transport/latest/9.2.csv] contains transport version name [dne] which is not defined")
    }

    def "latest files id must exist in definition"() {
        given:
        latestTransportVersion("9.2", "existing_92", "8124000")
        when:
        def result = validateDefinitionsFails()
        then:
        assertDefinitionsFailure(result, "Latest transport version file " +
            "[myserver/src/main/resources/transport/latest/9.2.csv] has id 8124000 which is not in definition " +
            "[myserver/src/main/resources/transport/defined/existing_92.csv]")
    }

    def "latest files have latest id within base"() {
        given:
        latestTransportVersion("9.0", "seemingly_latest", "8110001")
        definedAndUsedTransportVersion("original", "8110000")
        definedAndUsedTransportVersion("seemingly_latest", "8111000,8110001")
        definedAndUsedTransportVersion("actual_latest", "8112000,8110002")
        when:
        def result = validateDefinitionsFails()
        then:
        assertDefinitionsFailure(result, "Latest transport version file " +
            "[myserver/src/main/resources/transport/latest/9.0.csv] has id 8110001 from [seemingly_latest] with base 8110000 " +
            "but another id 8110002 from [actual_latest] is later for that base")
    }

    def "latest files cannot change base id"() {
        given:
        definedAndUsedTransportVersion("original", "8013000")
        definedAndUsedTransportVersion("patch", "8015000,8013001")
        latestTransportVersion("9.1", "patch", "8013001")
        when:
        def result = validateDefinitionsFails()
        then:
        assertDefinitionsFailure(result, "Latest transport version file " +
            "[myserver/src/main/resources/transport/latest/9.1.csv] modifies base id from 8012000 to 8013000")
    }

    def "ids must be dense"() {
        given:
        definedAndUsedTransportVersion("original", "8013000")
        definedAndUsedTransportVersion("patch1", "8015000,8013002")
        latestTransportVersion("9.0", "patch1", "8013002")
        when:
        def result = validateDefinitionsFails()
        then:
        assertDefinitionsFailure(result, "Transport version base id 8013000 is missing patch ids between 8013000 and 8013002")
    }

    def "primary id must not be patch version"() {
        given:
        definedAndUsedTransportVersion("patch", "8015001")
        when:
        def result = validateDefinitionsFails()
        then:
        assertDefinitionsFailure(result, "Transport version definition file " +
            "[myserver/src/main/resources/transport/defined/patch.csv] has patch version 8015001 as primary id")
    }
}
