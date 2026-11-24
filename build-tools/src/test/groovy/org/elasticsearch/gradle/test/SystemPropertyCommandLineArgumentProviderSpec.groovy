/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.test

import org.gradle.api.provider.Provider
import org.gradle.testfixtures.ProjectBuilder

import spock.lang.Specification

import java.util.function.Supplier

class SystemPropertyCommandLineArgumentProviderSpec extends Specification {

    def "resolves Gradle providers to their values"() {
        given:
        def project = ProjectBuilder.builder().build()
        def seed = project.provider { "DEADBEEF" }
        def properties = new SystemPropertyCommandLineArgumentProvider()

        when:
        properties.systemProperty("tests.seed", seed)
        properties.systemProperty("test.base", seed.map { s -> "base_" + s })

        then:
        properties.asArguments() == ["-Dtests.seed=DEADBEEF", "-Dtest.base=base_DEADBEEF"]
    }

    def "resolves a provider that is not also a Supplier"() {
        given:
        // Gradle's own providers happen to implement Supplier through an internal interface. This one does not, so it
        // checks that providers are resolved through the public Provider API rather than by that accident.
        Provider<String> provider = [get: { "DEADBEEF" }] as Provider<String>
        def properties = new SystemPropertyCommandLineArgumentProvider()

        when:
        properties.systemProperty("tests.seed", provider)

        then:
        (provider instanceof Supplier) == false
        properties.asArguments() == ["-Dtests.seed=DEADBEEF"]
    }

    def "resolves suppliers and passes plain values through"() {
        given:
        def properties = new SystemPropertyCommandLineArgumentProvider()

        when:
        properties.systemProperty("supplied", { -> "from-supplier" } as Supplier<String>)
        properties.systemProperty("plain", 42)

        then:
        properties.asArguments() == ["-Dsupplied=from-supplier", "-Dplain=42"]
    }
}
