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

class TransportVersionGenerationFuncTest extends AbstractTransportVersionFuncTest {
    def "test setup works"() {
        when:
        def result = gradleRunner("generateTransportVersionDefinition").build()
        then:
        result.task(":myserver:generateTransportVersionDefinition").outcome == TaskOutcome.SUCCESS
    }

    def "A definition should be generated for an undefined reference"() {
        given:
        String tvName = "potato_tv"
        when:
        def result = gradleRunner(
                "generateTransportVersionDefinition",
                "--name=" + tvName,
                "--increment=1",
                "--branches=main"
        ).build()
        then:
        result.task(":myserver:generateTransportVersionDefinition").outcome == TaskOutcome.SUCCESS
        validateDefinitionFile(tvName, List.of("9.2"))
    }

    def "Missing required arguments should fail, state should remain unaltered"() {
        // TODO
    }

    // TODO add a check for the increment, primary and patch
    void validateDefinitionFile(String definitionName, List<String> branches) {
        String filename = "myserver/src/main/resources/transport/definitions/named/" + definitionName + ".csv"
        assert file(filename).exists()

        String definitionFileText = file(filename).text.strip()
        assert definitionFileText.isEmpty() == false: "The definition file must not be empty"
        List<Integer> definitionIDs = Arrays.stream(definitionFileText.split(",")).map(Integer::valueOf).toList()
        assert branches.size() == definitionIDs.size(): "The definition file does not have an id for each branch"

        def latestNamesToIds = branches.stream()
                .map { file("myserver/src/main/resources/transport/latest/${it}.csv").text }
                .map { it.strip().split(",") }
                .map { new Tuple2(it.first(), Integer.valueOf(it.last())) }
                .toList()

        for (int i = 0; i < definitionIDs.size(); i++) {
            int definitionID = definitionIDs[i]
            String nameInLatest = latestNamesToIds[i].getV1()
            def idInLatest = latestNamesToIds[i].getV2()

            assert definitionName.equals(nameInLatest): "The latest and definition names must match"
            assert definitionID == idInLatest: "The latest and definition ids must match"
        }
    }

    /*
    TODO: Add tests that check that:
        - TVs added ontop of main in git, but are no longer referenced, are deleted
        - name without branches param should fail
        - branches without name param should fail (+ other invalid combos)
        - multiple branches should create patch versions
        - a single branch value should create only a primary id
        - a latest file without a corresponding definition file should be reverted to main
        - a merge conflict should be resolved, resulting in regeneration of the latest file.
        -
     */
}
