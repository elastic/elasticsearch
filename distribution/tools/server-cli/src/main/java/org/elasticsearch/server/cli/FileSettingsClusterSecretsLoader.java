/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.settings.ClusterSecrets;
import org.elasticsearch.common.settings.SecureClusterStateSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.env.Environment;
import org.elasticsearch.reservedstate.ReservedStateHandler;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static java.util.Objects.requireNonNullElse;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xcontent.XContentType.JSON;

/**
 * Secure settings loader which loads an implementation of {@link SecureSettings} that
 * load secrets from {@code cluster_secrets} of the locally mounted reserved state settings.
 */
public class FileSettingsClusterSecretsLoader implements SecureSettingsLoader {

    @Override
    public LoadedSecrets load(Environment environment, Terminal terminal) {
        Path fileSettings = ReservedStateHandler.reservedStateSettingsPath(environment);
        var reservedStateSecrets = readClusterSecrets(fileSettings);
        if (reservedStateSecrets != SecureClusterStateSettings.EMPTY) {
            terminal.println("Using cluster secrets from file settings [" + fileSettings + "]");
            return new LoadedSecrets(reservedStateSecrets, Optional.empty());
        }

        terminal.println("No cluster secrets available from file settings [" + fileSettings + "]");
        return new LoadedSecrets(SecureClusterStateSettings.EMPTY, Optional.empty());
    }

    @Override
    public SecureSettings bootstrap(Environment environment, SecureString password) {
        throw new IllegalArgumentException("Bootstrapping cluster secrets in file settings is not supported");
    }

    @Override
    public boolean supportsSecurityAutoConfiguration() {
        return false;
    }

    /**
     * Reads {@code cluster_secrets} from the provided settings file, returning {@link SecureClusterStateSettings#EMPTY}
     * if it doesn't exist.
     *
     * <p>An example {@code settings.json} might contain the following:
     * <pre>
     * {
     *     "state": {
     *         "cluster_secrets": {
     *             "string_secrets": {
     *                 "secure.setting.key.one": "aaa",
     *                 "secure.setting.key.two": "bbb"
     *             },
     *             "file_secrets": {
     *                 "secure.setting.key.three": "Y2Nj"
     *             }
     *         }
     *     }
     * }
     * </pre>
     */
    private static SecureClusterStateSettings readClusterSecrets(Path settingsFile) {
        if (Files.exists(settingsFile) == false) {
            return SecureClusterStateSettings.EMPTY;
        }
        try (
            var bis = new BufferedInputStream(Files.newInputStream(settingsFile));
            var parser = JSON.xContent().createParser(XContentParserConfiguration.EMPTY, bis)
        ) {
            return requireNonNullElse(createFileSettingsParser().apply(parser, null), SecureClusterStateSettings.EMPTY);
        } catch (IOException e) {
            throw new IllegalStateException("Error processing reserved state settings file", e);
        }
    }

    // one-off parser used to load initial cluster secrets
    private static ConstructingObjectParser<SecureClusterStateSettings, Void> createFileSettingsParser() {
        var stateParser = new ConstructingObjectParser<SecureClusterStateSettings, Void>(
            "state",
            true,
            a -> (SecureClusterStateSettings) a[0]
        );
        stateParser.declareObject(
            optionalConstructorArg(),
            (p, c) -> SecureClusterStateSettings.fromXContent(p),
            new ParseField(ClusterSecrets.NAME)
        );

        var parser = new ConstructingObjectParser<SecureClusterStateSettings, Void>(
            "file_settings",
            true,
            a -> (SecureClusterStateSettings) a[0]
        );
        parser.declareObject(optionalConstructorArg(), stateParser::apply, new ParseField("state"));
        return parser;
    }
}
