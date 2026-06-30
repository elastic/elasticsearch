/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal

import spock.lang.Specification
import spock.lang.Unroll

import org.gradle.api.InvalidUserDataException

/**
 * Unit tests for the pure-logic helpers in {@link InternalDistributionBwcSetupPlugin}.
 * These tests require no Gradle project, no TestKit, and no network access.
 */
class InternalDistributionBwcSetupPluginSpec extends Specification {

    // -------------------------------------------------------------------------
    // validateBwcMode
    // -------------------------------------------------------------------------

    @Unroll
    def "validateBwcMode accepts valid mode '#mode'"() {
        when:
        InternalDistributionBwcSetupPlugin.validateBwcMode(mode)

        then:
        noExceptionThrown()

        where:
        mode << ["gradle", "dra", "auto"]
    }

    @Unroll
    def "validateBwcMode rejects invalid mode '#mode'"() {
        when:
        InternalDistributionBwcSetupPlugin.validateBwcMode(mode)

        then:
        def ex = thrown(InvalidUserDataException)
        ex.message.contains("Invalid tests.bwc.mode value [${mode}]")
        ex.message.contains("Must be one of: gradle, dra, auto")

        where:
        mode << ["invalid-mode", "DRA", "AUTO", "", "dra-or-gradle"]
    }

    // -------------------------------------------------------------------------
    // buildFallbackMessage — distribution archives
    // -------------------------------------------------------------------------

    def "buildFallbackMessage for dra mode and distribution archive mentions DRA unavailable"() {
        when:
        def msg = InternalDistributionBwcSetupPlugin.buildFallbackMessage("dra", true, "9.4.2", "darwin-tar")

        then:
        msg.contains("tests.bwc.mode=dra but no DRA snapshot was available")
        msg.contains("9.4.2")
        msg.contains("darwin-tar")
        msg.contains("building distribution")
    }

    def "buildFallbackMessage for auto mode and distribution archive mentions commit mismatch"() {
        when:
        def msg = InternalDistributionBwcSetupPlugin.buildFallbackMessage("auto", true, "9.4.2", "linux-tar")

        then:
        msg.contains("DRA snapshot commit did not match")
        msg.contains("9.4.2")
        msg.contains("linux-tar")
    }

    def "buildFallbackMessage for gradle mode and distribution archive hints at auto mode"() {
        when:
        def msg = InternalDistributionBwcSetupPlugin.buildFallbackMessage("gradle", true, "9.4.2", "windows-zip")

        then:
        msg.contains("-Dtests.bwc.mode=auto")
        msg.contains("9.4.2")
    }

    // -------------------------------------------------------------------------
    // buildFallbackMessage — Maven JAR artifacts (non-archive)
    // -------------------------------------------------------------------------

    def "buildFallbackMessage for dra mode and Maven JAR mentions DRA unavailable"() {
        when:
        def msg = InternalDistributionBwcSetupPlugin.buildFallbackMessage("dra", false, "9.4.2", "jdbc")

        then:
        msg.contains("tests.bwc.mode=dra but no DRA snapshot was available")
        msg.contains("9.4.2")
        msg.contains("jdbc")
        // Maven JAR path does NOT say "distribution" — keep the messages distinct
        !msg.contains("building distribution")
    }

    def "buildFallbackMessage for gradle mode and Maven JAR emits a plain source-build message"() {
        when:
        def msg = InternalDistributionBwcSetupPlugin.buildFallbackMessage("gradle", false, "9.4.2", "logging")

        then:
        msg == "BWC [9.4.2]: building [logging] from source"
    }

    def "buildFallbackMessage for auto mode and Maven JAR emits a plain source-build message"() {
        when:
        def msg = InternalDistributionBwcSetupPlugin.buildFallbackMessage("auto", false, "9.3.5", "plugin-api")

        then:
        msg == "BWC [9.3.5]: building [plugin-api] from source"
    }
}
