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

import java.nio.file.Files
import java.nio.file.Path
import java.security.KeyStore
import java.security.MessageDigest

/**
 * Runs {@link BundledJdkCacertsTask} the way {@code distribution/build.gradle} does for the bundled JDKs, using the
 * {@code cacerts} of the JDK running the test as the input.
 */
class BundledJdkCacertsTaskFuncTest extends AbstractGradleFuncTest {

    Map<String, String> jdkRoots

    def setup() {
        Path cacerts = Path.of(System.getProperty("java.home"), "lib", "security", "cacerts")
        File copy = file("jdk/lib/security/cacerts")
        copy.bytes = Files.readAllBytes(cacerts)
        jdkRoots = fingerprints(copy)
        buildFile << """
            import org.elasticsearch.gradle.internal.distribution.BundledJdkCacertsTask

            plugins {
              id 'base'
              // we need to add one of our plugins to get things on this classpath
              id 'elasticsearch.global-build-info' apply false
            }

            def roots = providers.gradleProperty('roots').map { it.split(',') as List }

            tasks.register("cacerts", BundledJdkCacertsTask) {
              jdkCacerts.from(fileTree('jdk').matching { include 'lib/security/cacerts' })
              removedRoots.set(roots)
              outputFile.set(layout.buildDirectory.file('cacerts'))
            }
        """
    }

    def "removes the given roots and keeps all others"() {
        given:
        List<String> removed = jdkRoots.keySet().sort().take(2)

        when:
        def result = gradleRunner("cacerts", "-Proots=${removed.join(',')}").build()

        then:
        result.task(":cacerts").outcome == TaskOutcome.SUCCESS
        fingerprints(file("build/cacerts")) == jdkRoots.findAll { removed.contains(it.key) == false }
    }

    def "writes a trust store without a password that is reproducible"() {
        given:
        String removed = jdkRoots.keySet().sort().first()

        when:
        gradleRunner("cacerts", "-Proots=${removed}").build()
        byte[] first = file("build/cacerts").bytes
        gradleRunner("cacerts", "-Proots=${removed}", "--rerun-tasks").build()

        then:
        file("build/cacerts").bytes == first
        // a key store without a password can be loaded with any password
        load(file("build/cacerts"), "changeit".toCharArray()).size() == jdkRoots.size() - 1
    }

    def "fails for roots the JDK does not have"() {
        given:
        String present = jdkRoots.keySet().sort().first()
        String absent = '0' * 64

        when:
        def result = gradleRunner("cacerts", "-Proots=${present},${absent}").buildAndFail()

        then:
        result.output.contains("does not hold [${absent}]. Drop them from the list if the JDK no longer has them.")
        file("build/cacerts").exists() == false
    }

    def "reruns when the removed roots change"() {
        given:
        List<String> roots = jdkRoots.keySet().sort().take(2)
        gradleRunner("cacerts", "-Proots=${roots[0]}").build()

        when:
        def unchanged = gradleRunner("cacerts", "-Proots=${roots[0]}").build()
        def changed = gradleRunner("cacerts", "-Proots=${roots[1]}").build()

        then:
        unchanged.task(":cacerts").outcome == TaskOutcome.UP_TO_DATE
        changed.task(":cacerts").outcome == TaskOutcome.SUCCESS
        fingerprints(file("build/cacerts")).keySet() == jdkRoots.keySet() - roots[1]
    }

    def "rejects malformed fingerprint #fingerprint"() {
        when:
        def result = gradleRunner("cacerts", "-Proots=${fingerprint}").buildAndFail()

        then:
        result.output.contains("[${fingerprint}] is not an upper case hex SHA-256 certificate fingerprint")

        where:
        fingerprint << ["CAFE", ('a' * 64), ('AB:' * 31) + 'AB']
    }

    def "fails when the JDK has no cacerts file"() {
        given:
        file("jdk/lib/security/cacerts").delete()

        when:
        def result = gradleRunner("cacerts", "-Proots=${'0' * 64}").buildAndFail()

        then:
        result.output.contains("Expected exactly one cacerts file but found []")
    }

    private static Map<String, String> fingerprints(File trustStore) {
        KeyStore keyStore = load(trustStore, null)
        MessageDigest sha256 = MessageDigest.getInstance("SHA-256")
        return Collections.list(keyStore.aliases()).collectEntries { alias ->
            [(sha256.digest(keyStore.getCertificate(alias).encoded).encodeHex().toString().toUpperCase(Locale.ROOT)): alias]
        }
    }

    private static KeyStore load(File trustStore, char[] password) {
        KeyStore keyStore = KeyStore.getInstance("PKCS12")
        trustStore.withInputStream { keyStore.load(it, password) }
        return keyStore
    }
}
