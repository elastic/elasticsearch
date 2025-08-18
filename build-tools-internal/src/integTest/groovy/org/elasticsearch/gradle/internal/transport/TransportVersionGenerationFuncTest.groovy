/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport

import org.elasticsearch.gradle.util.Pair
import org.gradle.internal.impldep.com.google.common.collect.Streams
import org.gradle.testkit.runner.TaskOutcome

import java.util.stream.Stream

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
        def result = gradleRunner("generateTransportVersionDefinition", "--name=" + tvName, "--increment=1").build()
        then:
        result.task(":myserver:generateTransportVersionDefinition").outcome == TaskOutcome.SUCCESS
        validateDefinitionFile(tvName, List.of("9.2"))
    }

    void validateDefinitionFile(String name, List<String> branches) { // TODO add primary increment
        String filename = "myserver/src/main/resources/transport/definitions/named/" + name + ".csv"
        assert file(filename).exists()

        String contents = file(filename).text
        assert contents.strip().isEmpty() == false
        String[] x = contents.strip().split(",")
        Stream<Integer> ids = Arrays.stream(x).map(Integer::valueOf)
        assert branches.size() == ids.count(): "The definition file does not have the correct number of ids"

        def latestInfo = branches.stream()
                .map { file("myserver/src/main/resources/transport/latest/${it}.csv").text }
                .map { contents.strip().split(",") }
                .map { Pair<String, Integer>.of(it.first(), Integer.valueOf(it.last())) }

        Streams.zip(ids, latestInfo, { (id, latest) -> Pair.of(id, latest) })
                .forEach {
                    (id, Pair latest) -> {
                        assert name == latest.left(): "The latest file should contain the same name as the new transport version"
                        assert id == latest.right(): "The latest file should contain the same id as the new transport version"
                    }
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
