/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.release

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import spock.lang.Specification
import spock.lang.Subject

import org.elasticsearch.gradle.Version
import org.elasticsearch.gradle.internal.info.DevelopmentBranch
import org.gradle.api.Project
import org.gradle.testfixtures.ProjectBuilder

class UpdateBranchesJsonTaskTest extends Specification {

    @Subject
    UpdateBranchesJsonTask task

    File projectRoot
    Project project
    JsonSlurper jsonSlurper = new JsonSlurper()

    def setup() {
        projectRoot = File.createTempDir("update-branches-json-test")
        projectRoot.deleteOnExit()

        project = ProjectBuilder.builder().withProjectDir(projectRoot).build()
        task = project.tasks.register("updateBranchesJson", UpdateBranchesJsonTask).get()
    }

    def "should add new branch to branches.json"() {
        given:
        branchesJson([
            new DevelopmentBranch("main", Version.fromString("9.1.0")),
            new DevelopmentBranch("8.18", Version.fromString("8.18.2")),
            new DevelopmentBranch("8.17", Version.fromString("8.17.7")),
        ])
        task.addBranch('8.19:8.19.0')

        when:
        task.executeTask()

        then:
        def branchesFile = new File(projectRoot, "branches.json")
        def json = jsonSlurper.parse(branchesFile)
        json.branches == [
                [branch: 'main', version: '9.1.0'],
                [branch: '8.19', version: '8.19.0'],
                [branch: '8.18', version: '8.18.2'],
                [branch: '8.17', version: '8.17.7'],
        ]
    }

    void branchesJson(List<DevelopmentBranch> branches) {
        File branchesFile = new File(projectRoot, "branches.json")
        Map<String, Object> branchesFileContent = [
            branches: branches.collect { branch ->
                [
                    branch: branch.name(),
                    version: branch.version().toString(),
                ]
            }
        ]
        branchesFile.text = JsonOutput.prettyPrint(JsonOutput.toJson(branchesFileContent))

    }
}
