/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class FipsOptionsTests extends ESTestCase {

    private static final Settings FIPS = Settings.builder().put(FipsOptions.FIPS_MODE_SETTING, true).build();

    private Path esHome;

    @Before
    public void createHome() throws Exception {
        // a space in the path is the case that broke whitespace-split option strings
        esHome = createTempDir("es home");
        Files.createDirectories(esHome.resolve("lib"));
    }

    private void shipFipsPayload() throws IOException {
        Path fips = FipsOptions.fipsLib(esHome);
        Files.createDirectories(fips);
        Files.writeString(fips.resolve(FipsOptions.JAVA_SECURITY_FILE), "keystore.type=BCFKS\n");
        Files.write(fips.resolve(FipsOptions.CACERTS_FILE), new byte[0]);
        Files.write(fips.resolve("bc-fips-2.0.2.jar"), new byte[0]);
    }

    public void testNotInFipsMode() throws Exception {
        shipFipsPayload();
        assertThat(FipsOptions.systemProperties(Settings.EMPTY, esHome), empty());
        assertThat(FipsOptions.systemProperties(Settings.builder().put(FipsOptions.FIPS_MODE_SETTING, false).build(), esHome), empty());
    }

    public void testFipsModeWithShippedProvider() throws Exception {
        shipFipsPayload();
        Path fips = FipsOptions.fipsLib(esHome);
        List<String> properties = FipsOptions.systemProperties(FIPS, esHome);
        assertThat(properties, hasSize(6));
        assertThat(properties, hasItem("-Djava.security.properties==" + fips.resolve(FipsOptions.JAVA_SECURITY_FILE)));
        assertThat(properties, hasItem("-Dorg.bouncycastle.fips.approved_only=true"));
        assertThat(properties, hasItem("-Djavax.net.ssl.keyStoreType=BCFKS"));
        assertThat(properties, hasItem("-Djavax.net.ssl.trustStoreType=BCFKS"));
        assertThat(properties, hasItem("-Djavax.net.ssl.trustStore=" + fips.resolve(FipsOptions.CACERTS_FILE)));
        assertThat(properties, hasItem("-Djavax.net.ssl.trustStorePassword=" + FipsOptions.CACERTS_PASSWORD));
    }

    /** The manual setup copies the provider into lib; the shipped copy must then stay out of the way. */
    public void testOperatorProviderInLibWins() throws Exception {
        shipFipsPayload();
        Files.write(esHome.resolve("lib").resolve("bc-fips-2.0.0.jar"), new byte[0]);
        assertTrue(FipsOptions.hasOperatorProvider(esHome));
        assertThat(FipsOptions.systemProperties(FIPS, esHome), empty());
    }

    public void testOtherJarsInLibAreNotTheProvider() throws Exception {
        shipFipsPayload();
        Files.write(esHome.resolve("lib").resolve("bcpkix-jdk18on-1.78.jar"), new byte[0]);
        Files.write(esHome.resolve("lib").resolve("bc-fips-notes.txt"), new byte[0]);
        assertFalse(FipsOptions.hasOperatorProvider(esHome));
        assertThat(FipsOptions.systemProperties(FIPS, esHome), hasSize(6));
    }

    /** Distributions without the payload, and a lib/fips that lost its configuration, fail closed rather than start without FIPS. */
    public void testFipsModeWithoutShippedProvider() throws Exception {
        UserException e = expectThrows(UserException.class, () -> FipsOptions.systemProperties(FIPS, esHome));
        assertThat(e.exitCode, equalTo(ExitCodes.CONFIG));
        assertThat(e.getMessage(), containsString(FipsOptions.FIPS_MODE_SETTING + " is true but"));
        assertThat(e.getMessage(), containsString("available in the Linux distributions"));

        Files.createDirectories(FipsOptions.fipsLib(esHome));
        expectThrows(UserException.class, () -> FipsOptions.systemProperties(FIPS, esHome));
    }

    public void testOperatorSetPropertiesAreKept() throws Exception {
        shipFipsPayload();
        List<String> fipsProperties = FipsOptions.systemProperties(FIPS, esHome);
        List<String> jvmOptions = List.of(
            "-Xmx1g",
            "-Djava.security.properties==/etc/elasticsearch/my_java.security",
            "-Djavax.net.ssl.trustStore=/etc/elasticsearch/my_truststore.bcfks"
        );

        List<String> result = FipsOptions.withSystemProperties(jvmOptions, fipsProperties);

        assertThat(result.subList(0, jvmOptions.size()), equalTo(jvmOptions));
        assertThat(result, hasSize(jvmOptions.size() + 4));
        assertThat(result, not(hasItem(containsString(FipsOptions.JAVA_SECURITY_FILE))));
        assertThat(result, not(hasItem(containsString(FipsOptions.CACERTS_FILE))));
        assertThat(result, hasItem("-Dorg.bouncycastle.fips.approved_only=true"));
        assertThat(result, hasItem("-Djavax.net.ssl.trustStorePassword=" + FipsOptions.CACERTS_PASSWORD));
    }

    public void testWithNoFipsPropertiesIsUnchanged() {
        List<String> jvmOptions = List.of("-Xmx1g", "-Djava.security.properties=/x");
        assertThat(FipsOptions.withSystemProperties(jvmOptions, List.of()), equalTo(jvmOptions));
    }
}
