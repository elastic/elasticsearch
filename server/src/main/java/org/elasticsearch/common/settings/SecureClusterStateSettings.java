/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Map.entry;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Implementation of {@link SecureSettings} that represents secrets in cluster state. Secrets are stored as byte arrays along with
 * their SHA-256 digests. Provides functionality to read and serialize secure settings to broadcast them as part of cluster state.
 * Does not provide any encryption.
 *
 * <p>Cluster state secrets are initialized from file settings (typically using the {@code "cluster_secrets"} namespace),
 * and might look as follows under the respective namespace:
 * <pre>
 * {
 *     "string_secrets": {
 *         "secure.setting.key.one": "aaa",
 *         "secure.setting.key.two": "bbb"
 *     }
 *     "file_secrets": {
 *         "secure.setting.key.three": "Y2Nj"
 *     }
 * }
 * </pre>
 */
public class SecureClusterStateSettings implements SecureSettings {

    // a shared, empty instance that cannot be closed
    public static final SecureClusterStateSettings EMPTY = new SecureClusterStateSettings(Collections.emptyMap());

    // nullable (if closed), but otherwise immutable secrets map
    private @Nullable Map<String, Secret> secrets;
    private final Set<String> secretNames;

    /**
     * Do NOT use, this will be removed as part of ES-13910.
     * @deprecated  For testing, use {@code new MockSecureSettings().toSecureClusterStateSettings()} instead.
     */
    @Deprecated
    @SuppressWarnings("unchecked")
    public SecureClusterStateSettings(SecureSettings secureSettings) {
        this(
            Map.ofEntries(
                secureSettings.getSettingNames()
                    .stream()
                    .map(key -> entry(key, new Secret(getValueAsByteArray(secureSettings, key), getSHA256Digest(secureSettings, key))))
                    .toArray(Map.Entry[]::new)
            )
        );
    }

    public SecureClusterStateSettings(StreamInput in) throws IOException {
        this(in.readImmutableMap(v -> new Secret(in.readByteArray(), in.readByteArray())));
    }

    private SecureClusterStateSettings(Map<String, Secret> immutableSecrets) {
        this.secrets = immutableSecrets;
        this.secretNames = Set.of(immutableSecrets.keySet().toArray(new String[0]));
    }

    private SecureClusterStateSettings(SecureClusterStateSettings secureSettings) {
        this.secrets = secureSettings.secrets;
        this.secretNames = secureSettings.secretNames;
    }

    /**
     * Creates a copy of the given {@link SecureClusterStateSettings} sharing the immutable state,
     * but allowing the copy to be closed without impacting the original.
     */
    public static SecureClusterStateSettings copyOf(SecureClusterStateSettings secureSettings) {
        return new SecureClusterStateSettings(secureSettings);
    }

    /**
     * Reads secure cluster state settings from Json xContent, which might look as follows:
     * <pre>
     * {
     *     "string_secrets": {
     *         "secure.setting.key.one": "aaa",
     *         "secure.setting.key.two": "bbb"
     *     }
     *     "file_secrets": {
     *         "secure.setting.key.three": "Y2Nj"
     *     }
     * }
     * </pre>
     */
    public static SecureClusterStateSettings fromXContent(XContentParser parser) throws IOException {
        return ParserHolder.SECRETS_PARSER.apply(parser, null);
    }

    private Map<String, Secret> validSecrets() {
        Map<String, Secret> current = secrets;
        if (current == null) {
            throw new IllegalStateException("SecureClusterStateSettings already closed");
        }
        return current;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(validSecrets(), StreamOutput::writeString, (o, v) -> v.writeTo(o));
    }

    @Override
    public boolean isLoaded() {
        return true;
    }

    @Override
    public Set<String> getSettingNames() {
        return secretNames;
    }

    @Override
    public SecureString getString(String setting) {
        Secret value = validSecrets().get(setting);
        if (value == null) {
            return null;
        }
        ByteBuffer byteBuffer = ByteBuffer.wrap(value.secret());
        CharBuffer charBuffer = StandardCharsets.UTF_8.decode(byteBuffer);
        return new SecureString(Arrays.copyOfRange(charBuffer.array(), charBuffer.position(), charBuffer.limit()));
    }

    @Override
    public InputStream getFile(String setting) {
        var value = validSecrets().get(setting);
        if (value == null) {
            return null;
        }
        return new ByteArrayInputStream(value.secret());
    }

    @Override
    public byte[] getSHA256Digest(String setting) {
        return validSecrets().get(setting).sha256Digest();
    }

    @Override
    public void close() {
        if (this != EMPTY) {
            secrets = null;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SecureClusterStateSettings secrets1 = (SecureClusterStateSettings) o;
        return Objects.equals(secrets, secrets1.secrets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(secrets);
    }

    private static byte[] getValueAsByteArray(SecureSettings secureSettings, String key) {
        try (var is = secureSettings.getFile(key)) {
            return is.readAllBytes();
        } catch (IOException | GeneralSecurityException e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] getSHA256Digest(SecureSettings secureSettings, String key) {
        try {
            return secureSettings.getSHA256Digest(key);
        } catch (GeneralSecurityException e) {
            throw new RuntimeException(e);
        }
    }

    private record Secret(byte[] secret, byte[] sha256Digest) implements Writeable {

        private static Secret stringSecret(String secret) {
            byte[] bytes = secret.getBytes(StandardCharsets.UTF_8);
            return new Secret(bytes, MessageDigests.sha256().digest(bytes));
        }

        private static Secret fileSecret(String secret) {
            byte[] bytes = Base64.getDecoder().decode(secret);
            return new Secret(bytes, MessageDigests.sha256().digest(bytes));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Secret entry = (Secret) o;
            return Arrays.equals(secret, entry.secret) && Arrays.equals(sha256Digest, entry.sha256Digest);
        }

        @Override
        public int hashCode() {
            int result = Arrays.hashCode(secret);
            result = 31 * result + Arrays.hashCode(sha256Digest);
            return result;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByteArray(secret);
            out.writeByteArray(sha256Digest);
        }
    }

    private interface ParserHolder {
        ConstructingObjectParser<SecureClusterStateSettings, Void> SECRETS_PARSER = createSecretsParser();

        @SuppressWarnings({ "unchecked", "rawtypes" })
        private static ConstructingObjectParser<SecureClusterStateSettings, Void> createSecretsParser() {
            ConstructingObjectParser<SecureClusterStateSettings, Void> secretsParser = new ConstructingObjectParser<>(
                "secrets_parser",
                a -> {
                    Map<String, String> stringSecrets = a[0] == null ? Collections.emptyMap() : (Map<String, String>) a[0];
                    Map<String, String> fileSecrets = a[1] == null ? Collections.emptyMap() : (Map<String, String>) a[1];

                    Set<String> duplicateKeys = Sets.intersection(stringSecrets.keySet(), fileSecrets.keySet());
                    if (duplicateKeys.isEmpty() == false) {
                        throw new IllegalStateException("Some settings were defined as both string and file settings: " + duplicateKeys);
                    }

                    Map.Entry[] entries = new Map.Entry[stringSecrets.size() + fileSecrets.size()];
                    int i = 0;
                    for (Map.Entry<String, String> entry : stringSecrets.entrySet()) {
                        entries[i++] = entry(entry.getKey(), Secret.stringSecret(entry.getValue()));
                    }
                    for (Map.Entry<String, String> entry : fileSecrets.entrySet()) {
                        entries[i++] = entry(entry.getKey(), Secret.fileSecret(entry.getValue()));
                    }
                    return new SecureClusterStateSettings(Map.ofEntries(entries));
                }
            );

            ParseField stringSecretsField = new ParseField("string_secrets");
            ParseField fileSecretsField = new ParseField("file_secrets");
            secretsParser.declareObject(optionalConstructorArg(), (p, c) -> p.map(), stringSecretsField);
            secretsParser.declareObject(optionalConstructorArg(), (p, c) -> p.map(), fileSecretsField);
            return secretsParser;
        }
    }
}
