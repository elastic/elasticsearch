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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * The JVM options that run a JVM on the Bouncy Castle FIPS provider shipped in {@code lib/fips}.
 *
 * <p> FIPS 140 mode is switched on by the node setting {@code xpack.security.fips_mode.enabled}; there is no environment
 * variable. The provider ships dormant in {@code lib/fips}, a directory that is neither on the server module path nor matched
 * by the class path wildcard of the command line tools, so a node that is not in FIPS mode never loads it. When the setting is
 * true, {@link ServerCli} adds the directory to the server module path and these properties to the server JVM options, and
 * {@link FipsOptionsCommand} prints the same properties for {@code bin/elasticsearch-cli} to pass to a command line tool.
 *
 * <p> An operator who followed the manual FIPS setup has copied their own copy of the provider into {@code lib} and carries the
 * JVM options in {@code jvm.options} and {@code CLI_JAVA_OPTS}. Such a node gets nothing from here, so that an upgrade does not
 * put two copies of the provider on the module path, and so that its own options keep applying.
 */
final class FipsOptions {

    static final String FIPS_MODE_SETTING = "xpack.security.fips_mode.enabled";

    /** The {@code java.security} replacement in {@code lib/fips}, loaded with {@code ==} so that the JDK's providers are dropped. */
    static final String JAVA_SECURITY_FILE = "fips_java.security";

    /** The bundled JDK's CA bundle converted to BCFKS, the only store type an approved-only provider can open. */
    static final String CACERTS_FILE = "cacerts.bcfks";

    /** Protects nothing secret, the store holds public certificates, but the provider wants at least 14 characters. */
    static final String CACERTS_PASSWORD = "passwordcacert";

    private FipsOptions() {}

    static boolean isFipsMode(Settings settings) {
        return settings.getAsBoolean(FIPS_MODE_SETTING, false);
    }

    static Path fipsLib(Path esHome) {
        return esHome.resolve("lib").resolve("fips");
    }

    /**
     * @return true when the operator installed their own copy of the Bouncy Castle FIPS provider in {@code lib}, the documented
     *         manual setup, in which case the shipped copy must stay dormant
     */
    static boolean hasOperatorProvider(Path esHome) throws IOException {
        Path lib = esHome.resolve("lib");
        if (Files.isDirectory(lib) == false) {
            return false;
        }
        try (Stream<Path> jars = Files.list(lib)) {
            return jars.anyMatch(p -> {
                String name = p.getFileName().toString();
                return name.startsWith("bc-fips-") && name.endsWith(".jar");
            });
        }
    }

    /**
     * The system properties that put a JVM on the shipped provider, in approved-only mode, with the JDK's own providers replaced
     * and the BCFKS CA bundle as the default trust store.
     *
     * @return the properties, or an empty list when the node is not in FIPS mode or the operator manages the provider themselves
     * @throws UserException when FIPS mode is on but this distribution does not ship the provider
     */
    static List<String> systemProperties(Settings settings, Path esHome) throws UserException, IOException {
        if (isFipsMode(settings) == false || hasOperatorProvider(esHome)) {
            return List.of();
        }
        Path lib = fipsLib(esHome);
        if (Files.isRegularFile(lib.resolve(JAVA_SECURITY_FILE)) == false) {
            throw new UserException(
                ExitCodes.CONFIG,
                FIPS_MODE_SETTING
                    + " is true but "
                    + lib
                    + " does not contain the FIPS provider; FIPS 140 mode is available in the Linux distributions"
            );
        }
        return List.of(
            "-Djava.security.properties==" + lib.resolve(JAVA_SECURITY_FILE),
            "-Dorg.bouncycastle.fips.approved_only=true",
            "-Djavax.net.ssl.keyStoreType=BCFKS",
            "-Djavax.net.ssl.trustStoreType=BCFKS",
            "-Djavax.net.ssl.trustStore=" + lib.resolve(CACERTS_FILE),
            "-Djavax.net.ssl.trustStorePassword=" + CACERTS_PASSWORD
        );
    }

    /**
     * Appends the FIPS properties to JVM options, skipping any property the operator already set. The JVM takes the last value of
     * a repeated {@code -D}, so appending without the check would override the operator's choice.
     */
    static List<String> withSystemProperties(List<String> jvmOptions, List<String> fipsProperties) {
        List<String> result = new ArrayList<>(jvmOptions);
        for (String property : fipsProperties) {
            String key = property.substring(0, property.indexOf('=') + 1);
            if (jvmOptions.stream().noneMatch(option -> option.startsWith(key))) {
                result.add(property);
            }
        }
        return result;
    }
}
