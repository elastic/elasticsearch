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
import org.gradle.api.InvalidUserDataException
import org.gradle.api.Project
import org.gradle.testfixtures.ProjectBuilder
import spock.lang.TempDir

class UpdateBranchesJsonTaskSpec extends Specification {

    @Subject
    UpdateBranchesJsonTask task

    @TempDir
    File projectRoot
    JsonSlurper jsonSlurper = new JsonSlurper()

    def setup() {
        Project project = ProjectBuilder.builder().withProjectDir(projectRoot).build()
        task = project.tasks.register("updateBranchesJson", UpdateBranchesJsonTask).get()
        task.branchesFile = new File(projectRoot, "branches.json")
    }

    def "add new branch to branches.json (sorted by version) when --add-branch is specified"() {
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

    def "remove branch from branches.json when --remove-branch is specified"() {
        given:
        branchesJson([
            new DevelopmentBranch("main", Version.fromString("9.1.0")),
            new DevelopmentBranch("8.18", Version.fromString("8.18.2")),
            new DevelopmentBranch("8.17", Version.fromString("8.17.7")),
        ])
        task.removeBranch('8.18')

        when:
        task.executeTask()

        then:
        def branchesFile = new File(projectRoot, "branches.json")
        def json = jsonSlurper.parse(branchesFile)
        json.branches == [
                [branch: 'main', version: '9.1.0'],
                [branch: '8.17', version: '8.17.7'],
        ]
    }

    def "update branch version when --update-branch is specified"() {
        given:
        branchesJson([
            new DevelopmentBranch("main", Version.fromString("9.1.0")),
            new DevelopmentBranch("8.18", Version.fromString("8.18.2")),
            new DevelopmentBranch("8.17", Version.fromString("8.17.7")),
        ])
        task.updateBranch('8.18:8.18.3')

        when:
        task.executeTask()

        then:
        def branchesFile = new File(projectRoot, "branches.json")
        def json = jsonSlurper.parse(branchesFile)
        json.branches == [
                [branch: 'main', version: '9.1.0'],
                [branch: '8.18', version: '8.18.3'],
                [branch: '8.17', version: '8.17.7'],
        ]
    }

    def "change branches.json when multiple options are specified"() {
        given:
        branchesJson([
            new DevelopmentBranch("main", Version.fromString("9.1.0")),
            new DevelopmentBranch("8.18", Version.fromString("8.18.2")),
            new DevelopmentBranch("8.17", Version.fromString("8.17.7")),
        ])
        task.addBranch('8.19:8.19.0')
        task.removeBranch('8.18')
        task.updateBranch('8.17:8.17.8')

        when:
        task.executeTask()

        then:
        def branchesFile = new File(projectRoot, "branches.json")
        def json = jsonSlurper.parse(branchesFile)
        json.branches == [
                [branch: 'main', version: '9.1.0'],
                [branch: '8.19', version: '8.19.0'],
                [branch: '8.17', version: '8.17.8'],
        ]
    }

    def "fail when no --add-branch, --remove-branch or --update-branch is specified"() {
        given:
        branchesJson([
            new DevelopmentBranch("main", Version.fromString("9.1.0")),
            new DevelopmentBranch("8.18", Version.fromString("8.18.2")),
            new DevelopmentBranch("8.17", Version.fromString("8.17.7")),
        ])

        when:
        task.executeTask()

        then:
        def branchesFile = new File(projectRoot, "branches.json")
        def json = jsonSlurper.parse(branchesFile)
        json.branches == [
            [branch: 'main', version: '9.1.0'],
            [branch: '8.18', version: '8.18.2'],
            [branch: '8.17', version: '8.17.7'],
        ]
        thrown(InvalidUserDataException)
    }

    def "fail when adding a branch that already exists"() {
        given:
        branchesJson([
            new DevelopmentBranch("main", Version.fromString("9.1.0")),
            new DevelopmentBranch("8.18", Version.fromString("8.18.2")),
            new DevelopmentBranch("8.17", Version.fromString("8.17.7")),
        ])
        task.addBranch('8.18:8.18.3')

        when:
        task.executeTask()

        then:
        def branchesFile = new File(projectRoot, "branches.json")
        def json = jsonSlurper.parse(branchesFile)
        json.branches == [
                [branch: 'main', version: '9.1.0'],
                [branch: '8.18', version: '8.18.2'],
                [branch: '8.17', version: '8.17.7'],
        ]
        thrown(InvalidUserDataException)
    }

    def "fail when removing a branch that does not exist"() {
        given:
        branchesJson([
            new DevelopmentBranch("main", Version.fromString("9.1.0")),
            new DevelopmentBranch("8.18", Version.fromString("8.18.2")),
            new DevelopmentBranch("8.17", Version.fromString("8.17.7")),
        ])
        task.removeBranch('8.19')

        when:
        task.executeTask()

        then:
        def branchesFile = new File(projectRoot, "branches.json")
        def json = jsonSlurper.parse(branchesFile)
        json.branches == [
                [branch: 'main', version: '9.1.0'],
                [branch: '8.18', version: '8.18.2'],
                [branch: '8.17', version: '8.17.7'],
        ]
        thrown(InvalidUserDataException)
    }

    def "fail when updating a branch that does not exist"() {
        given:
        branchesJson([
            new DevelopmentBranch("main", Version.fromString("9.1.0")),
            new DevelopmentBranch("8.18", Version.fromString("8.18.2")),
            new DevelopmentBranch("8.17", Version.fromString("8.17.7")),
        ])
        task.updateBranch('8.19:8.19.0')

        when:
        task.executeTask()

        then:
        def branchesFile = new File(projectRoot, "branches.json")
        def json = jsonSlurper.parse(branchesFile)
        json.branches == [
                [branch: 'main', version: '9.1.0'],
                [branch: '8.18', version: '8.18.2'],
                [branch: '8.17', version: '8.17.7'],
        ]
        thrown(InvalidUserDataException)
    }

    def "fail when adding a branch with an invalid version"() {
        given:
        branchesJson([
            new DevelopmentBranch("main", Version.fromString("9.1.0")),
            new DevelopmentBranch("8.18", Version.fromString("8.18.2")),
            new DevelopmentBranch("8.17", Version.fromString("8.17.7")),
        ])

        when:
        task.addBranch('8.19:invalid')
        task.executeTask()

        then:
        def branchesFile = new File(projectRoot, "branches.json")
        def json = jsonSlurper.parse(branchesFile)
        json.branches == [
                [branch: 'main', version: '9.1.0'],
                [branch: '8.18', version: '8.18.2'],
                [branch: '8.17', version: '8.17.7'],
        ]
        thrown(IllegalArgumentException)
    }

    def "fail when branches.json is missing"() {
        given:
        task.updateBranch('8.19:8.19.0')

        when:
        task.executeTask()

        then:
        def exception = thrown(InvalidUserDataException)
        exception.message.contains("branches.json has not been found")
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
