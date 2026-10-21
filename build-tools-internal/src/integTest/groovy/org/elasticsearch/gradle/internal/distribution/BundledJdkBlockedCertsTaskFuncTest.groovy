/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.distribution

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome

/**
 * Runs {@link BundledJdkBlockedCertsTask} against a fake JDK {@code blocked.certs} file the way
 * {@code distribution/build.gradle} does for the bundled JDKs.
 */
class BundledJdkBlockedCertsTaskFuncTest extends AbstractGradleFuncTest {

    static final String JDK_BLOCKED = "1C5E6985ACC09221DBD1A4B7BBC6D3A8C3F8540D19F20763A9537FDD42B4FFE7"
    static final String JDK_BLOCKED_TOO = "03DB9E5E79FE6117177F81C11595AF598CB176AF766290DBCEB2C318B32E39A2"
    static final String ADDITIONAL = "42C4ACCD12C4F59597925E1A08A7A90102608543809308D50EC427E118BA1493"

    def setup() {
        file("jdk/lib/security/blocked.certs").text = "Algorithm=SHA-256\n${JDK_BLOCKED_TOO}\n${JDK_BLOCKED}\n"
        file("jdk/lib/security/cacerts").text = "not relevant"
        buildFile << """
            import org.elasticsearch.gradle.internal.distribution.BundledJdkBlockedCertsTask

            plugins {
              id 'base'
              // we need to add one of our plugins to get things on this classpath
              id 'elasticsearch.global-build-info' apply false
            }

            def fingerprints = providers.gradleProperty('fingerprints').map { it.split(',') as List }

            tasks.register("blockedCerts", BundledJdkBlockedCertsTask) {
              jdkBlockedCerts.from(fileTree('jdk').matching { include 'lib/security/blocked.certs' })
              additionalFingerprints.set(fingerprints)
              outputFile.set(layout.buildDirectory.file('blocked.certs'))
            }
        """
    }

    def "adds fingerprints to the ones the JDK blocks"() {
        when:
        def result = gradleRunner("blockedCerts", "-Pfingerprints=${ADDITIONAL},${JDK_BLOCKED}").build()

        then:
        result.task(":blockedCerts").outcome == TaskOutcome.SUCCESS
        file("build/blocked.certs").text == "Algorithm=SHA-256\n${JDK_BLOCKED_TOO}\n${JDK_BLOCKED}\n${ADDITIONAL}\n"
    }

    def "reruns when the fingerprints change"() {
        given:
        gradleRunner("blockedCerts", "-Pfingerprints=${ADDITIONAL}").build()

        when:
        def unchanged = gradleRunner("blockedCerts", "-Pfingerprints=${ADDITIONAL}").build()
        def changed = gradleRunner("blockedCerts", "-Pfingerprints=${JDK_BLOCKED}").build()

        then:
        unchanged.task(":blockedCerts").outcome == TaskOutcome.UP_TO_DATE
        changed.task(":blockedCerts").outcome == TaskOutcome.SUCCESS
        file("build/blocked.certs").text == "Algorithm=SHA-256\n${JDK_BLOCKED_TOO}\n${JDK_BLOCKED}\n"
    }

    def "rejects malformed fingerprint #fingerprint"() {
        when:
        def result = gradleRunner("blockedCerts", "-Pfingerprints=${fingerprint}").buildAndFail()

        then:
        result.output.contains("[${fingerprint}] is not an upper case hex SHA-256 certificate fingerprint")

        where:
        fingerprint << ["CAFE", ADDITIONAL.toLowerCase(Locale.ROOT), ADDITIONAL.toList().collate(2)*.join().join(':')]
    }

    def "rejects a blocked.certs file in an unexpected format"() {
        given:
        file("jdk/lib/security/blocked.certs").text = "Algorithm=SHA-1\n${JDK_BLOCKED}\n"

        when:
        def result = gradleRunner("blockedCerts", "-Pfingerprints=${ADDITIONAL}").buildAndFail()

        then:
        result.output.contains("to start with [Algorithm=SHA-256]")
    }

    def "rejects unexpected lines in the JDK's blocked.certs file"() {
        given:
        file("jdk/lib/security/blocked.certs").text = "Algorithm=SHA-256\n# a comment\n${JDK_BLOCKED}\n"

        when:
        def result = gradleRunner("blockedCerts", "-Pfingerprints=${ADDITIONAL}").buildAndFail()

        then:
        result.output.contains("Unexpected line [# a comment]")
    }

    def "fails when the JDK has no blocked.certs file"() {
        given:
        file("jdk/lib/security/blocked.certs").delete()

        when:
        def result = gradleRunner("blockedCerts", "-Pfingerprints=${ADDITIONAL}").buildAndFail()

        then:
        result.output.contains("Expected exactly one blocked.certs file but found []")
    }
}
