/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.util

import org.gradle.api.GradleException
import org.gradle.testfixtures.ProjectBuilder
import spock.lang.Specification

/**
 * Covers the failure path of {@link ParamsUtils#loadBuildParams}: plugins that need build parameters
 * must fail with an actionable message when {@code elasticsearch.global-build-info} has not been applied
 * to the root project, rather than with a generic unknown-domain-object error.
 */
class ParamsUtilsSpec extends Specification {

    def "loadBuildParams fails with an actionable message when the buildParams service is not registered"() {
        given:
        def project = ProjectBuilder.builder().withName("sub").build()

        when:
        ParamsUtils.loadBuildParams(project)

        then:
        def e = thrown(GradleException)
        e.message.contains("Build service 'buildParams' is not registered")
        e.message.contains("elasticsearch.global-build-info")
    }
}
