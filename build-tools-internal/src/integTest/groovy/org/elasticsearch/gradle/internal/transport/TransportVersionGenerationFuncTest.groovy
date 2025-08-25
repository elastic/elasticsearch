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
        return gradleRunner(args.toArray()).build()
    }

    void assertGenerateAndValidateSuccess(BuildResult result) {
        assert result.task(":myserver:generateTransportVersionDefinition").outcome == TaskOutcome.SUCCESS
        assert result.task(":myserver:validateTransportVersionDefinitions").outcome == TaskOutcome.SUCCESS
    }

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

    def "a definition should be generated when specified by an arg but no code reference exists"(List<String> branches) {
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

    def "a definition should be generated when the name arg isn't specified but a code reference exists"() {
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

    def "generation should fail if the branches arg is omitted, the state should remain unaltered"() {
        when:
        def generateResult = gradleRunner("generateTransportVersionDefinition", "--name=no_branches").buildAndFail()
        def validateResult = gradleRunner("validateTransportVersionDefinitions").build()

        then:
        generateResult.task(":myserver:generateTransportVersionDefinition").outcome == TaskOutcome.FAILED
        validateResult.task(":myserver:validateTransportVersionDefinitions").outcome == TaskOutcome.SUCCESS
    }

    def "latest file modifications should be reverted to their original state on main"(
            List<String> branchesParam,
            List<String> latestFilesModified,
            String name
    ) {
        given:
        List<LatestFile> originalModifiedLatestFiles = readLatestFiles(latestFilesModified)
        List<LatestFile> originalBranchesLatestFiles = readLatestFiles(branchesParam)

        when: "We modify the latest files"
        originalModifiedLatestFiles.forEach {
            latestTransportVersion(it.branch, it.name + "_modification", (it.id + 7).toString())
        }

        and: "We run the generation task"
        def args = [
                ":myserver:validateTransportVersionDefinitions",
                ":myserver:generateTransportVersionDefinition"
        ]
        if (branchesParam != null) {
            args.add("--branches=" + branchesParam.join(","))
        }
        if (name != null) {
            referencedTransportVersion(name)
            args.add("--name=" + name)
        }

        def result = gradleRunner(args.toArray(new String[0])).build()

        then: "The generation and validation tasks should succeed"
        result.task(":myserver:generateTransportVersionDefinition").outcome == TaskOutcome.SUCCESS
        result.task(":myserver:validateTransportVersionDefinitions").outcome == TaskOutcome.SUCCESS

        and: "The modified latest files should be reverted if there is no name specified or they are not specified in the branches param"
        originalModifiedLatestFiles.forEach { originalLatest ->
            boolean noNameSpecified = name == null
            boolean modifiedNotInBranchesParam = branchesParam != null && branchesParam.contains(originalLatest.branch) == false
            if (noNameSpecified || modifiedNotInBranchesParam) {
                def latest = readLatestFile(originalLatest.branch)
                assert latest.branch == originalLatest.branch
                assert latest.id == originalLatest.id
            }
        }

        and: "The latest files for the branches param should be incremented correctly"
        if (name != null) {
            validateDefinitionFile(name, originalBranchesLatestFiles,)
        }

        where:
        branchesParam  | latestFilesModified | name
        ["9.2"]        | ["9.1"]             | "test_tv" // TODO legitimate bug, need to always clean up latest.
        ["9.2"] | ["9.1"] | null // TODO legitimate bug, need to always clean up latest.
        ["9.2", "9.1"] | ["9.2", "9.1"]      | "test_tv"

    }

    def "irrelevant changes to the latest file should be reverted"() {
        given:
        latestTransportVersion("9.2", "modification", "9000000")

        when:
        def result = runGenerateAndValidateTask("--branches=9.2")

        then:
        assertGenerateAndValidateSuccess(result)
        assertLatest("9.2", "existing_92,8123000")
    }

    def "irrelevant changes to multiple latest files should be reverted"() {
        given:
        latestTransportVersion("9.2", "modification", "9000000")
        latestTransportVersion("9.1", "modification", "9000000")

        when:
        def result = runGenerateAndValidateTask("--branches=9.2,9.1")

        then:
        assertGenerateAndValidateSuccess(result)
        assertLatest("9.2", "existing_92,8123000")
        assertLatest("9.1", "existing_92,8012001")
    }

    def "when a reference is removed after a definition is generated, the definition should be deleted and latest files reverted"(List<String> branches) {
        given:
        String definitionName = "test_tv_patch_ids"
        namedTransportVersion(definitionName, "8124000")
        latestTransportVersion("9.2", definitionName, "8124000")

        when: "generation is run"
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
//                ["9.2", "9.1"] // TODO add another test for this, since this is a different setup
        ]
    }

    def "when a reference is renamed after a definition was generated, the original should be removed and latest files updated"(List<String> branches) {
        given:
        String firstName = "original_tv_name"
        namedTransportVersion(firstName, "8124000")
        latestTransportVersion("9.2", firstName, "8124000")
        String secondName = "new_tv_name"
        referencedTransportVersion(secondName)

        when:
        def secondResult = gradleRunner(
                ":myserver:validateTransportVersionDefinitions",
                ":myserver:generateTransportVersionDefinition",
                "--branches=" + branches.join(",")
        ).build()

        then: "The generation task should succeed and the definition file should be deleted"
        !file("myserver/src/main/resources/transport/definitions/named/${firstName}.csv").exists()
        file("myserver/src/main/resources/transport/definitions/named/${secondName}.csv").exists()
        secondResult.task(":myserver:generateTransportVersionDefinition").outcome == TaskOutcome.SUCCESS
        secondResult.task(":myserver:validateTransportVersionDefinitions").outcome == TaskOutcome.SUCCESS
        file("myserver/src/main/resources/transport/latest/9.2.csv").text.strip() == "new_tv_name,8124000"

        where:
        branches << [
                ["9.2"],
                ["9.2", "9.1"]
        ]
    }

    def "when a definition file is deleted and the reference and latest files haven't been changed, the system should regenerate"(List<String> branches) {
        given:
        String definitionName = "test_tv"
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
        def latestFilesAfterGeneration = readLatestFiles(branches)

        when: "The definition file is deleted"
        file("myserver/src/main/resources/transport/definitions/named/${definitionName}.csv").delete()

        then: "The definition file should no longer exist"
        !file("myserver/src/main/resources/transport/definitions/named/${definitionName}.csv").exists()

        when: "Validation is run"
        def validationResult = gradleRunner(":myserver:validateTransportVersionDefinitions").buildAndFail()

        then: "The validation task should fail since the definition file is missing"
        validationResult.task(":myserver:validateTransportVersionDefinitions").outcome == TaskOutcome.FAILED

        when: "The generation task is run again"
        def secondResult = gradleRunner(
                ":myserver:validateTransportVersionDefinitions",
                ":myserver:generateTransportVersionDefinition",
                "--branches=" + branches.join(",")
        ).build()

        then: "The generation task should succeed and the definition file should be recreated"
        secondResult.task(":myserver:generateTransportVersionDefinition").outcome == TaskOutcome.SUCCESS
        secondResult.task(":myserver:validateTransportVersionDefinitions").outcome == TaskOutcome.SUCCESS
        validateDefinitionFile(definitionName, originalLatestFiles)

        and: "The latest files should remain unchanged"
        latestFilesAfterGeneration.forEach { originalLatest ->
            def latest = readLatestFile(originalLatest.branch)
            assert latest.branch == originalLatest.branch
            assert latest.id == originalLatest.id
            assert latest.name == originalLatest.name
        }

        where:
        branches << [
                ["9.2"],
                ["9.2", "9.1"]
        ]
    }

    def "when a latest file is incorrectly changed and a referenced definition file exists, the latest file should be regenerated"(List<String> branches) {
        given:
        String definitionName = "test_tv"
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

        when: "The latest files are modified"
        List<LatestFile> latestFilesAfterGeneration = readLatestFiles(branches)
        originalLatestFiles.forEach {
            latestTransportVersion(it.branch, it.name + "_modification", (it.id + 7).toString())
        }

        and: "The generation task is run again"
        def secondResult = gradleRunner(
                ":myserver:validateTransportVersionDefinitions",
                ":myserver:generateTransportVersionDefinition",
                "--branches=" + branches.join(",")
        ).build()

        then: "The generation task should succeed and the latest files should be reverted and incremented correctly"
        secondResult.task(":myserver:generateTransportVersionDefinition").outcome == TaskOutcome.SUCCESS
        secondResult.task(":myserver:validateTransportVersionDefinitions").outcome == TaskOutcome.SUCCESS
        validateDefinitionFile(definitionName, originalLatestFiles)

        latestFilesAfterGeneration.forEach { originalLatest ->
            def latest = readLatestFile(originalLatest.branch)
            assert latest.branch == originalLatest.branch
            assert latest.id == originalLatest.id
            assert latest.name == originalLatest.name
        }

        where:
        branches << [
                ["9.2"],
                ["9.2", "9.1"]
        ]
    }

    def "when a definition is created with a patch version, then generation is called without the patch version, the latest patch file should be reverted"() {
        given:
        String definitionName = "test_tv"
        referencedTransportVersion(definitionName)
        List<String> branches = ["9.2", "9.1"]
        List<String> mainBranch = ["9.2"]
        List<LatestFile> originalLatestFilesWithPatch = readLatestFiles(branches)
        List<LatestFile> originalLatestMainFile = readLatestFiles(mainBranch)
        LatestFile originalLatestPatchFile = readLatestFile("9.1")

        when: "The definition is generated with a patch version"
        def result = gradleRunner(
                ":myserver:validateTransportVersionDefinitions",
                ":myserver:generateTransportVersionDefinition",
                "--name=" + definitionName,
                "--branches=" + branches.join(",")
        ).build()

        then: "The generation task should succeed and create the definition file"
        result.task(":myserver:generateTransportVersionDefinition").outcome == TaskOutcome.SUCCESS
        result.task(":myserver:validateTransportVersionDefinitions").outcome == TaskOutcome.SUCCESS
        validateDefinitionFile(definitionName, originalLatestFilesWithPatch)

        when: "The generation is called again without the patch version"
        def secondResult = gradleRunner(
                ":myserver:validateTransportVersionDefinitions",
                ":myserver:generateTransportVersionDefinition",
                "--branches=" + mainBranch.join(",")
        ).build()

        then: "The generation task should succeed and the definition file should be updated"
        secondResult.task(":myserver:generateTransportVersionDefinition").outcome == TaskOutcome.SUCCESS
        secondResult.task(":myserver:validateTransportVersionDefinitions").outcome == TaskOutcome.SUCCESS
        validateDefinitionFile(definitionName, originalLatestMainFile)

        and: "The latest file for the patch version should be reverted to the original state"
        LatestFile latestPatchFile = readLatestFile("9.1")
        latestPatchFile.id == originalLatestPatchFile.id
        latestPatchFile.name == originalLatestPatchFile.name
        latestPatchFile.branch == originalLatestPatchFile.branch
    }


    def "when a reference is deleted, the system should delete the definition and revert the latest files"(List<String> branches) {
        given:
        String definitionName = "test_tv"
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

        when: "The reference is deleted and the generation is run again"
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

        and: "The latest files should be reverted to their original state"
        originalLatestFiles.forEach { originalLatest ->
            def latest = readLatestFile(originalLatest.branch)
            assert latest.branch == originalLatest.branch
            assert latest.id == originalLatest.id
            assert latest.name == originalLatest.name
        }

        where:
        branches << [
                ["9.2"],
                ["9.2", "9.1"]
        ]
    }

    def "latest files mangled by a merge conflict should be regenerated, and the most recent definition file should be updated"() {
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
        def result = gradleRunner(
                ":myserver:validateTransportVersionDefinitions",
                ":myserver:generateTransportVersionDefinition",
                "--name=second_tv",
                "--branches=9.2"
        ).build()

        then: "The generation task should succeed and the latest files should be reverted and incremented correctly"
        result.task(":myserver:generateTransportVersionDefinition").outcome == TaskOutcome.SUCCESS
        result.task(":myserver:validateTransportVersionDefinitions").outcome == TaskOutcome.SUCCESS
        file("myserver/src/main/resources/transport/definitions/named/existing_92.csv").text.strip() == "8123000,8012001"
        file("myserver/src/main/resources/transport/definitions/named/second_tv.csv").text.strip() == "8124000"
        file("myserver/src/main/resources/transport/latest/9.2.csv").text.strip() == "second_tv,8124000"
    }

    // TODO do we need a test that has a garbled latest file but Main is OK, and it increments correctly?

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
