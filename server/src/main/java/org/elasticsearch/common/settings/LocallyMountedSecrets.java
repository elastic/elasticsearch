/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.reservedstate.service.ReservedStateVersion;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xcontent.XContentType.JSON;

/**
 * An implementation of {@link SecureSettings} which loads the secrets from
 * externally mounted local directory. It looks for the folder called 'secrets'
 * under the config directory. All secure settings should be supplied in a single
 * file called 'secrets.json' which sits inside the 'secrets' directory.
 * <p>
 * If the 'secrets' directory or the 'secrets.json' file don't exist, the
 * SecureSettings implementation is loaded with empty settings map.
 * <p>
 * Example secrets.json format:
 *         {
 *              "metadata": {
 *                  "version": "1",
 *                  "compatibility": "8.7.0"
 *              },
 *              "secrets": {
 *                  "secure.setting.key.one": "aaa",
 *                  "secure.setting.key.two": "bbb"
 *              }
 *         }
 */
public class LocallyMountedSecrets implements SecureSettings {

    public static final String SECRETS_FILE_NAME = "secrets.json";
    public static final String SECRETS_DIRECTORY = "secrets";

    public static final ParseField SECRETS_FIELD = new ParseField("secrets");
    public static final ParseField METADATA_FIELD = new ParseField("metadata");

    @SuppressWarnings("unchecked")
    private final ConstructingObjectParser<LocalFileSecrets, Void> secretsParser = new ConstructingObjectParser<>(
        "locally_mounted_secrets",
        a -> new LocalFileSecrets((Map<String, String>) a[0], (ReservedStateVersion) a[1])
    );

    private final String secretsDir;
    private final String secretsFile;
    private final SetOnce<LocalFileSecrets> secrets = new SetOnce<>();

    /**
     * Direct constructor to be used by the CLI
     */
    public LocallyMountedSecrets(Environment environment) {
        var secretsDirPath = environment.configFile().toAbsolutePath().resolve(SECRETS_DIRECTORY);
        var secretsFilePath = secretsDirPath.resolve(SECRETS_FILE_NAME);
        secretsParser.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.map(), SECRETS_FIELD);
        secretsParser.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> ReservedStateVersion.parse(p), METADATA_FIELD);
        if (Files.exists(secretsDirPath) && Files.exists(secretsFilePath)) {
            try {
                secrets.set(processSecretsFile(secretsFilePath));
            } catch (IOException e) {
                throw new IllegalStateException("Error processing secrets file", e);
            }
        } else {
            secrets.set(new LocalFileSecrets(Map.of(), new ReservedStateVersion(-1L, Version.CURRENT)));
        }
        this.secretsDir = secretsDirPath.toString();
        this.secretsFile = secretsFilePath.toString();
    }

    /**
     * Used by {@link org.elasticsearch.bootstrap.ServerArgs} to deserialize the secrets
     * when they are received by the Elasticsearch process. The ServerCli code serializes
     * the secrets as part of ServerArgs.
     */
    public LocallyMountedSecrets(StreamInput in) throws IOException {
        this.secretsDir = in.readString();
        this.secretsFile = in.readString();
        if (in.readBoolean()) {
            secrets.set(LocalFileSecrets.readFrom(in));
        }
        // TODO: Add support for watching for file changes here.
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(secretsDir);
        out.writeString(secretsFile);
        if (secrets.get() == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            secrets.get().writeTo(out);
        }
    }

    @Override
    public boolean isLoaded() {
        return secrets.get() != null;
    }

    @Override
    public Set<String> getSettingNames() {
        assert isLoaded();
        return secrets.get().map().keySet();
    }

    @Override
    public SecureString getString(String setting) {
        assert isLoaded();
        var value = secrets.get().map().get(setting);
        if (value == null) {
            return null;
        }
        return new SecureString(value.toCharArray());
    }

    @Override
    public InputStream getFile(String setting) throws GeneralSecurityException {
        assert isLoaded();
        return new ByteArrayInputStream(getString(setting).toString().getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public byte[] getSHA256Digest(String setting) throws GeneralSecurityException {
        assert isLoaded();
        return MessageDigests.sha256().digest(getString(setting).toString().getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void close() throws IOException {
        if (null != secrets.get() && secrets.get().map().isEmpty() == false) {
            for (var entry : secrets.get().map().entrySet()) {
                entry.setValue(null);
            }
        }
    }

    // package private for testing
    LocalFileSecrets processSecretsFile(Path path) throws IOException {
        try (
            var fis = Files.newInputStream(path);
            var bis = new BufferedInputStream(fis);
            var parser = JSON.xContent().createParser(XContentParserConfiguration.EMPTY, bis)
        ) {
            return secretsParser.apply(parser, null);
        }
    }

    record LocalFileSecrets(Map<String, String> map, ReservedStateVersion metadata) implements Writeable {
        public static LocalFileSecrets readFrom(StreamInput in) throws IOException {
            return new LocalFileSecrets(in.readMap(StreamInput::readString, StreamInput::readString), ReservedStateVersion.readFrom(in));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap((map == null) ? Map.of() : map, StreamOutput::writeString, StreamOutput::writeString);
            metadata.writeTo(out);
        }
    }
}
