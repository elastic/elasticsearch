/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.precommit

import org.gradle.api.Project
import org.gradle.testfixtures.ProjectBuilder
import spock.lang.Specification

class CheckForbiddenApisTaskSpec extends Specification {

    def "setBundledSignatures sorts values"() {
        given:
        Project project = ProjectBuilder.builder().build()
        CheckForbiddenApisTask task = project.tasks.register("checkForbiddenApis", CheckForbiddenApisTask).get()

        when:
        task.setBundledSignatures(["jdk-unsafe", "jdk-non-portable"])

        then:
        task.bundledSignatures.get().toList() == ["jdk-non-portable", "jdk-unsafe"]
    }

    def "configureBundledSignaturesFromDefaults updates and sorts"() {
        given:
        Project project = ProjectBuilder.builder().build()
        CheckForbiddenApisTask task = project.tasks.register("checkForbiddenApis", CheckForbiddenApisTask).get()

        when:
        task.configureBundledSignaturesFromDefaults { spec ->
            spec.remove("jdk-non-portable")
            spec.add("jdk-internal")
        }

        then:
        task.bundledSignatures.get().toList() == ["jdk-internal", "jdk-system-out", "jdk-unsafe"]
        !task.bundledSignatures.get().contains("jdk-non-portable")
    }
}
