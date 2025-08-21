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
        def result = gradleRunner(
                ":myserver:generateTransportVersionDefinition",
                ":myserver:validateTransportVersionDefinitions"
        ).build()

        then:
        result.task(":myserver:generateTransportVersionDefinition").outcome == TaskOutcome.SUCCESS
        result.task(":myserver:validateTransportVersionDefinitions").outcome == TaskOutcome.SUCCESS
    }

    def "A definition should be generated when specified by an arg but no code reference exists"(List<String> branches) {
        given:
        String tvName = "test_tv_patch_ids"
        List<LatestFile> latestBranchesToOriginalIds = readLatestFiles(branches)

        when: "generation is run with a name specified and no code references"
        def result = gradleRunner(
                "generateTransportVersionDefinition",
                "--name=" + tvName,
                "--branches=" + branches.join(",")
        ).build()

        then: "The generation task should succeed and create the definition file"
        result.task(":myserver:generateTransportVersionDefinition").outcome == TaskOutcome.SUCCESS
        validateDefinitionFile(tvName, latestBranchesToOriginalIds)

        when: "A reference is added"
        referencedTransportVersion(tvName)
        def validateResult = gradleRunner("validateTransportVersionDefinitions").build()

        then: "The full validation should succeed now that the reference exists"
        validateResult.task(":myserver:validateTransportVersionDefinitions").outcome == TaskOutcome.SUCCESS

        where:
        branches << [
                ["9.2"],
                ["9.2", "9.1"]
        ]
    }

    def "A definition should be generated when the name arg isn't specified but a code reference exists"() {
        given:
        String tvName = "test_tv_patch_ids"
        referencedTransportVersion(tvName)
        List<String> branches = ["9.2"]
        List<LatestFile> latestBranchesToOriginalIds = readLatestFiles(branches)

        when:
        def result = gradleRunner(
                ":myserver:validateTransportVersionDefinitions",
                "generateTransportVersionDefinition",
                "--branches=" + branches.join(",")
        ).build()

        then:
        result.task(":myserver:generateTransportVersionDefinition").outcome == TaskOutcome.SUCCESS
        result.task(":myserver:validateTransportVersionDefinitions").outcome == TaskOutcome.SUCCESS
        validateDefinitionFile(tvName, latestBranchesToOriginalIds)
    }

    def "Generation should fail if the branches arg is omitted, the state should remain unaltered"() {
        when:
        def generateResult = gradleRunner("generateTransportVersionDefinition", "--name=no_branches").buildAndFail()
        def validateResult = gradleRunner("validateTransportVersionDefinitions").build()

        then:
        generateResult.task(":myserver:generateTransportVersionDefinition").outcome == TaskOutcome.FAILED
        validateResult.task(":myserver:validateTransportVersionDefinitions").outcome == TaskOutcome.SUCCESS
    }

    def "Latest file modifications should be reverted to their original state on main"(
            List<String> branchesParam,
            List<String> latestFilesModified,
            String name
    ) {
        given:
        def originalLatestFiles = latestFilesModified.stream().map { readLatestFile(it) }.toList()

        when: "generation should revert the latest files to their original state"
        originalLatestFiles.forEach {
            latestTransportVersion(it.branch, it.name + "_modification", (it.id + 1).toString())
        }
        def args = [
                ":myserver:validateTransportVersionDefinitions",
                ":myserver:generateTransportVersionDefinition"
        ]
        if (branchesParam != null) {
            args.add("--branches=" + branchesParam.join(","))
        }
        if (name != null) {
            referencedTransportVersion(name)
        }
        def result = gradleRunner(args.toArray(new String[0])).build()

        then: "The generation and validation tasks should succeed, and the latest files should be reverted"
        result.task(":myserver:generateTransportVersionDefinition").outcome == TaskOutcome.SUCCESS
        result.task(":myserver:validateTransportVersionDefinitions").outcome == TaskOutcome.SUCCESS
        originalLatestFiles.forEach { originalLatest ->
            def latest = readLatestFile(originalLatest.branch)
            assert latest.branch == originalLatest.branch
            assert latest.id == originalLatest.id
        }
        // TODO assert that the definition file is created if a name is specified, and contains the correct incremented IDs

        where:
        branchesParam  | latestFilesModified | name
//        null           | ["9.2"]             | null
//        null           | ["9.2", "9.1"]      | null
//        ["9.2", "9.1"] | ["9.2"]             | null
        ["9.2"]        | ["9.1"]             | "test_tv" // TODO legitimate bug?
    }

    // TODO this test is finding a legitimate bug
    def "definitions that are no longer referenced should be deleted and latest files reverted"(List<String> branches) {
        given:
        String definitionName = "test_tv_patch_ids"
        referencedTransportVersion(definitionName)
        List<LatestFile> originalLatestFiles = readLatestFiles(branches)

        when: "The definition is generated"
        def result = gradleRunner(
                ":myserver:validateTransportVersionDefinitions",
                ":myserver:generateTransportVersionDefinition",
                "--name=" + definitionName,
                "--branches=" + branches.join(",")
        ).build()

        then: "The generation task should succeed and create the definition file"
        result.task(":myserver:generateTransportVersionDefinition").outcome == TaskOutcome.SUCCESS
        result.task(":myserver:validateTransportVersionDefinitions").outcome == TaskOutcome.SUCCESS
        validateDefinitionFile(definitionName, originalLatestFiles)

        when: "The reference is removed and the generation is run again"
        deleteTransportVersionReference(definitionName)
        def secondResult = gradleRunner(
                ":myserver:validateTransportVersionDefinitions",
                ":myserver:generateTransportVersionDefinition",
                "--branches=" + branches.join(",")
        ).build()

        then: "The generation task should succeed and the definition file should be deleted"
        !file("myserver/src/main/resources/transport/definitions/named/${definitionName}.csv").exists()
        secondResult.task(":myserver:generateTransportVersionDefinition").outcome == TaskOutcome.SUCCESS
        secondResult.task(":myserver:validateTransportVersionDefinitions").outcome == TaskOutcome.SUCCESS

        where:
        branches << [
                ["9.2"],
                ["9.2", "9.1"]
        ]
    }

    def "unreferenced definition files should be deleted"() {
        given:
        namedTransportVersion("old_name", "8124000")
        referencedTransportVersion("new_name")

        when:
        def result = gradleRunner(
            ":myserver:validateTransportVersionDefinitions",
            ":myserver:generateTransportVersionDefinition",
            "--name=new_name" ,
            "--branches=9.2"
        ).build()

        then:
        !file("myserver/src/main/resources/transport/definitions/named/old_name.csv").exists()
        result.task(":myserver:generateTransportVersionDefinition").outcome == TaskOutcome.SUCCESS
        result.task(":myserver:validateTransportVersionDefinitions").outcome == TaskOutcome.SUCCESS
    }


    /*
    TODO: Add tests that check that:
        -
        - a latest file without a corresponding definition file should be reverted to main
        - a merge conflict should be resolved, resulting in regeneration of the latest file.
        - add a reference, generate, remove the reference, add another reference, generate
        -
     */

    List<LatestFile> readLatestFiles(List<String> branches) {
        return branches.stream()
                .map { readLatestFile(it) }
                .toList()
    }

    LatestFile readLatestFile(String branch) {
        String latestFileText = file(
                "myserver/src/main/resources/transport/latest/${branch}.csv"
        ).text.strip()
        assert latestFileText.isEmpty() == false: "The latest file must not be empty"
        List<String> parts = latestFileText.split(",")
        assert parts.size() == 2: "The latest file must contain exactly two parts"
        return new LatestFile(branch, parts[0], Integer.valueOf(parts[1]))
    }

    void validateDefinitionFile(String definitionName, List<LatestFile> originalLatestFiles, Integer primaryIncrement = 1000) {
        String filename = "myserver/src/main/resources/transport/definitions/named/" + definitionName + ".csv"
        assert file(filename).exists()

        String definitionFileText = file(filename).text.strip()
        assert definitionFileText.isEmpty() == false: "The definition file must not be empty"
        List<Integer> definitionIDs = Arrays.stream(definitionFileText.split(",")).map(Integer::valueOf).toList()
        assert originalLatestFiles.size() == definitionIDs.size(): "The definition file does not have an id for each latest file"

        def latestNamesToIds = originalLatestFiles.stream()
                .map { it.branch }
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

            int originalID = originalLatestFiles[i].id
            if (i == 0) {
                assert definitionID == originalID + primaryIncrement:
                        "The primary version ID should be incremented by ${primaryIncrement} from the main branch latest file"
            } else {
                assert definitionID == originalID + 1:
                        "The patch version ID should be incremented by 1 from the primary version latest file"
            }

        }
    }

    class LatestFile {
        String branch
        String name
        Integer id

        LatestFile(String branch, String name, Integer id) {
            this.branch = branch
            this.name = name
            this.id = id
        }

        @Override
        String toString() {
            return "LatestFile(branch=${branch}, name=${name}, id=${id})"
        }
    }
}
