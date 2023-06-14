/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.reservedstate.service.ReservedStateVersion;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
 *              "string_secrets": {
 *                  "secure.setting.key.one": "aaa",
 *                  "secure.setting.key.two": "bbb"
 *              }
 *              "file_secrets": {
 *                  "secure.setting.key.three": "Y2Nj"
 *              }
 *         }
 */
public class LocallyMountedSecrets implements SecureSettings {

    public static final String SECRETS_FILE_NAME = "secrets.json";
    public static final String SECRETS_DIRECTORY = "secrets";

    // TODO[wrb]: remove deprecated name once controller and performance have updated their formats
    public static final ParseField STRING_SECRETS_FIELD = new ParseField("string_secrets", "secrets");
    public static final ParseField FILE_SECRETS_FIELD = new ParseField("file_secrets");
    public static final ParseField METADATA_FIELD = new ParseField("metadata");

    @SuppressWarnings("unchecked")
    private final ConstructingObjectParser<LocalFileSecrets, Void> secretsParser = new ConstructingObjectParser<>(
        "locally_mounted_secrets",
        a -> {
            final var decoder = Base64.getDecoder();

            Map<String, byte[]> stringSecretsMap = a[0] == null
                ? Map.of()
                : ((Map<String, String>) a[0]).entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getBytes(StandardCharsets.UTF_8)));

            Map<String, byte[]> fileSecretsByteMap = a[1] == null
                ? Map.of()
                : ((Map<String, String>) a[1]).entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> decoder.decode(e.getValue())));

            Set<String> duplicateKeys = fileSecretsByteMap.keySet()
                .stream()
                .filter(stringSecretsMap::containsKey)
                .collect(Collectors.toSet());

            if (duplicateKeys.isEmpty() == false) {
                throw new IllegalStateException("Some settings were defined as both string and file settings: " + duplicateKeys);
            }

            Map<String, byte[]> allSecrets = Stream.concat(stringSecretsMap.entrySet().stream(), fileSecretsByteMap.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            return new LocalFileSecrets(allSecrets, (ReservedStateVersion) a[2]);
        }
    );

    private final String secretsDir;
    private final String secretsFile;
    private final SetOnce<LocalFileSecrets> secrets = new SetOnce<>();

    /**
     * Direct constructor to be used by the CLI
     */
    public LocallyMountedSecrets(Environment environment) {
        var secretsDirPath = resolveSecretsDir(environment);
        var secretsFilePath = resolveSecretsFile(environment);
        secretsParser.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), STRING_SECRETS_FIELD);
        secretsParser.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), FILE_SECRETS_FIELD);
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
     * Resolve a secrets directory path given an environment
     * @param environment Elasticsearch environment
     * @return Secrets directory within an Elasticsearch environment
     */
    public static Path resolveSecretsDir(Environment environment) {
        return environment.configFile().toAbsolutePath().resolve(SECRETS_DIRECTORY);
    }

    /**
     * Resolve a secure settings file path given an environment
     * @param environment Elasticsearch environment
     * @return Secure settings file within an Elasticsearch environment
     */
    public static Path resolveSecretsFile(Environment environment) {
        return resolveSecretsDir(environment).resolve(SECRETS_FILE_NAME);
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
        return secrets.get().entries().keySet();
    }

    @Override
    public SecureString getString(String setting) {
        assert isLoaded();
        var value = secrets.get().entries().get(setting);
        if (value == null) {
            return null;
        }
        ByteBuffer byteBuffer = ByteBuffer.wrap(value);
        CharBuffer charBuffer = StandardCharsets.UTF_8.decode(byteBuffer);
        return new SecureString(Arrays.copyOfRange(charBuffer.array(), charBuffer.position(), charBuffer.limit()));
    }

    @Override
    public InputStream getFile(String setting) throws GeneralSecurityException {
        assert isLoaded();
        return new ByteArrayInputStream(secrets.get().entries().get(setting));
    }

    @Override
    public byte[] getSHA256Digest(String setting) throws GeneralSecurityException {
        assert isLoaded();
        return MessageDigests.sha256().digest(secrets.get().entries().get(setting));
    }

    /**
     * Returns version number from the secrets file
     */
    public long getVersion() {
        return secrets.get().metadata.version();
    }

    @Override
    public void close() throws IOException {
        if (null != secrets.get() && secrets.get().entries().isEmpty() == false) {
            for (var entry : secrets.get().entries().entrySet()) {
                entry.setValue(null);
            }
        }
    }

    // package private for testing
    LocalFileSecrets processSecretsFile(Path path) throws IOException {
        try (
            var fis = Files.newInputStream(path);
            var bis = new BufferedInputStream(fis);
            var parser = JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY.withDeprecationHandler(DeprecationHandler.IGNORE_DEPRECATIONS), bis)
        ) {
            return secretsParser.apply(parser, null);
        }
    }

    record LocalFileSecrets(Map<String, byte[]> entries, ReservedStateVersion metadata) implements Writeable {

        /**
         * Read LocalFileSecrets from stream input
         *
         * <p>This class should only be used node-locally, to represent the local secrets on a particular
         * node. Thus, the transport version should always be {@link TransportVersion#current()}
         */
        public static LocalFileSecrets readFrom(StreamInput in) throws IOException {
            assert in.getTransportVersion() == TransportVersion.current();
            return new LocalFileSecrets(in.readMap(StreamInput::readString, StreamInput::readByteArray), ReservedStateVersion.readFrom(in));
        }

        /**
         * Write LocalFileSecrets to stream output
         *
         * <p>This class should only be used node-locally, to represent the local secrets on a particular
         * node. Thus, the transport version should always be {@link TransportVersion#current()}
         */
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            assert out.getTransportVersion() == TransportVersion.current();
            out.writeMap((entries == null) ? Map.of() : entries, StreamOutput::writeString, StreamOutput::writeByteArray);
            metadata.writeTo(out);
        }
    }
}
