/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.crypto;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser.ValueType;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.chunk;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Stores the primary encryption keys in project metadata.
 *
 * <p>The primary encryption key (PEK) is a randomly generated AES-256 key used to encrypt secrets stored in cluster state. The plaintext
 * PEK is held in-memory and distributed to all nodes via cluster state updates over the transport layer.
 *
 * <p>Keys are identified by a randomly generated key ID. Multiple keys are retained to support key rotation. The active key is explicitly
 * identified by {@link #getActiveKeyId()}. Each key carries a {@link KeyEntry#generatedAt()} timestamp. The active key's age drives the
 * next-rotation decision; non-active keys age out independently after a grace period before being retired.
 *
 * <p>This metadata is persisted to disk via the gateway ({@link Metadata.XContentContext#GATEWAY}) but is NOT exposed in REST APIs or
 * included in snapshots.
 */
public class PrimaryEncryptionKeyMetadata extends AbstractNamedDiffable<Metadata.ProjectCustom> implements Metadata.ProjectCustom {

    public static final String TYPE = "primary_encryption_key";

    public static final TransportVersion PRIMARY_ENCRYPTION_KEY_VERSION = TransportVersion.fromName("primary_encryption_key");
    public static final TransportVersion PRIMARY_ENCRYPTION_KEY_ROTATION = TransportVersion.fromName("primary_encryption_key_rotation");

    private static final int KEY_LENGTH_BYTES = 32;
    private static final String KEY_ALGORITHM = "AES";
    private static final ParseField KEYS_FIELD = new ParseField("keys");
    private static final ParseField ACTIVE_KEY_ID_FIELD = new ParseField("active_key_id");
    private static final ParseField BYTES_FIELD = new ParseField("bytes");
    private static final ParseField GENERATED_AT_FIELD = new ParseField("generated_at");

    /**
     * A single key with its generation timestamp
     */
    public record KeyEntry(byte[] bytes, long generatedAt) implements Writeable {

        public KeyEntry {
            Objects.requireNonNull(bytes, "bytes");
            if (bytes.length != KEY_LENGTH_BYTES) {
                throw new IllegalArgumentException("Key bytes must be " + KEY_LENGTH_BYTES + " bytes, got " + bytes.length);
            }
        }

        public KeyEntry(StreamInput in) throws IOException {
            this(in.readByteArray(), in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByteArray(bytes);
            out.writeVLong(generatedAt);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o instanceof KeyEntry(byte[] bytes1, long at)) {
                return generatedAt == at && Arrays.equals(bytes, bytes1);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(bytes), generatedAt);
        }
    }

    private final Map<String, KeyEntry> keys;
    private final String activeKeyId;

    public PrimaryEncryptionKeyMetadata(Map<String, KeyEntry> keys, String activeKeyId) {
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("Keys map must not be empty");
        }
        if (keys.containsKey(activeKeyId) == false) {
            throw new IllegalArgumentException("Active key ID [" + activeKeyId + "] not found in keys");
        }
        assert keys.values().stream().mapToLong(KeyEntry::generatedAt).max().orElse(Long.MIN_VALUE) == keys.get(activeKeyId).generatedAt()
            : "active key [" + activeKeyId + "] must be the newest entry by generatedAt in " + keys;
        this.keys = Map.copyOf(keys);
        this.activeKeyId = activeKeyId;
    }

    public PrimaryEncryptionKeyMetadata(StreamInput in) throws IOException {
        this(readEntriesFrom(in), in.readString());
    }

    private static Map<String, KeyEntry> readEntriesFrom(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(PRIMARY_ENCRYPTION_KEY_ROTATION)) {
            return in.readImmutableMap(StreamInput::readString, KeyEntry::new);
        }
        return in.readImmutableMap(StreamInput::readString, streamInput -> new KeyEntry(streamInput.readByteArray(), 0L));
    }

    /**
     * Generates a random key ID.
     */
    public static String generateKeyId() {
        return UUIDs.randomBase64UUID();
    }

    /**
     * Returns the active key's ID.
     */
    public String getActiveKeyId() {
        return activeKeyId;
    }

    /**
     * Returns a defensive copy of the raw key bytes for the specified key ID, or {@code null} if the key ID is not present.
     */
    public byte[] getKeyBytes(String keyId) {
        KeyEntry entry = keys.get(keyId);
        return entry != null ? entry.bytes().clone() : null;
    }

    /**
     * Returns the active key as a {@link SecretKey} suitable for use with AES cryptographic operations.
     */
    public SecretKey toSecretKey() {
        return new SecretKeySpec(keys.get(activeKeyId).bytes(), KEY_ALGORITHM);
    }

    /**
     * Returns the key for the specified key ID as a {@link SecretKey}, or {@code null} if the key ID is not present.
     */
    public SecretKey toSecretKey(String keyId) {
        KeyEntry entry = keys.get(keyId);
        return entry != null ? new SecretKeySpec(entry.bytes(), KEY_ALGORITHM) : null;
    }

    /**
     * Returns an unmodifiable view of the keys (key bytes + generation timestamps) keyed by key ID.
     */
    public Map<String, KeyEntry> getKeys() {
        return keys;
    }

    /**
     * Returns the time at which the specified key was generated, or {@code 0L} if the key ID is not present.
     */
    public long getGeneratedAt(String keyId) {
        KeyEntry entry = keys.get(keyId);
        return entry != null ? entry.generatedAt() : 0L;
    }

    /**
     * Returns the IDs of non-active keys that have been deactivated for long enough to retire.
     *
     * <p>A non-active key's "deactivation time" is the {@code generatedAt} of the next-newer key in the map (the key that replaced it).
     * The grace period is measured from that deactivation time.
     *
     * <p>A key is retire-eligible iff its deactivation time is strictly less than {@code cutoffDeactivationMillis}
     */
    public Set<String> findRetireableKeyIds(long cutoffDeactivationMillis) {
        // Sort by generatedAt ascending so each entry's deactivation time is the next entry's generatedAt.
        // The active key is always the newest entry, so it lands last and never has its successor inspected.
        List<Map.Entry<String, KeyEntry>> sorted = keys.entrySet()
            .stream()
            .sorted(Comparator.comparingLong(e -> e.getValue().generatedAt()))
            .toList();
        Set<String> retireable = new HashSet<>();
        for (int i = 0; i < sorted.size() - 1; i++) {
            String id = sorted.get(i).getKey();
            if (id.equals(activeKeyId)) {
                continue;
            }
            long deactivatedAt = sorted.get(i + 1).getValue().generatedAt();
            if (deactivatedAt < cutoffDeactivationMillis) {
                retireable.add(id);
            }
        }
        return retireable;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return EnumSet.of(Metadata.XContentContext.GATEWAY);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return PRIMARY_ENCRYPTION_KEY_VERSION;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(PRIMARY_ENCRYPTION_KEY_ROTATION)) {
            out.writeMap(keys, StreamOutput::writeString, (o, e) -> e.writeTo(o));
        } else {
            out.writeMap(keys, StreamOutput::writeString, (o, e) -> o.writeByteArray(e.bytes()));
        }
        out.writeString(activeKeyId);
    }

    public static NamedDiff<Metadata.ProjectCustom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Metadata.ProjectCustom.class, TYPE, in);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return chunk((builder, params) -> {
            builder.field(ACTIVE_KEY_ID_FIELD.getPreferredName(), activeKeyId);
            builder.startObject(KEYS_FIELD.getPreferredName());
            for (Map.Entry<String, KeyEntry> entry : keys.entrySet()) {
                builder.startObject(entry.getKey());
                builder.field(BYTES_FIELD.getPreferredName(), entry.getValue().bytes());
                builder.field(GENERATED_AT_FIELD.getPreferredName(), entry.getValue().generatedAt());
                builder.endObject();
            }
            builder.endObject();
            return builder;
        });
    }

    private static final ConstructingObjectParser<KeyEntry, String> KEY_ENTRY_PARSER = new ConstructingObjectParser<>(
        "key_entry",
        false,
        (args, name) -> new KeyEntry((byte[]) args[0], (long) args[1])
    );
    static {
        KEY_ENTRY_PARSER.declareField(constructorArg(), XContentParser::binaryValue, BYTES_FIELD, ValueType.VALUE);
        KEY_ENTRY_PARSER.declareLong(constructorArg(), GENERATED_AT_FIELD);
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<PrimaryEncryptionKeyMetadata, Void> PARSER = new ConstructingObjectParser<>(
        TYPE,
        false,
        args -> {
            String activeKeyId = (String) args[0];
            List<Tuple<String, KeyEntry>> list = (List<Tuple<String, KeyEntry>>) args[1];
            Map<String, KeyEntry> keys = list.stream().collect(Collectors.toMap(Tuple::v1, Tuple::v2));
            return new PrimaryEncryptionKeyMetadata(keys, activeKeyId);
        }
    );
    static {
        PARSER.declareString(constructorArg(), ACTIVE_KEY_ID_FIELD);
        PARSER.declareNamedObjects(constructorArg(), (p, c, name) -> {
            XContentParser.Token valueToken = p.nextToken();
            if (valueToken == XContentParser.Token.START_OBJECT) {
                return Tuple.tuple(name, KEY_ENTRY_PARSER.apply(p, name));
            }
            return Tuple.tuple(name, new KeyEntry(p.binaryValue(), 0L)); // old format
        }, KEYS_FIELD);
    }

    public static Metadata.ProjectCustom fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public String toString() {
        StringBuilder ts = new StringBuilder();
        for (String id : new TreeSet<>(keys.keySet())) {
            if (!ts.isEmpty()) {
                ts.append(", ");
            }
            ts.append(id).append('=').append(keys.get(id).generatedAt());
        }
        return "PrimaryEncryptionKeyMetadata{activeKeyId="
            + activeKeyId
            + ", keyCount="
            + keys.size()
            + ", generatedAt="
            + ts
            + ", [keys secret]}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PrimaryEncryptionKeyMetadata that = (PrimaryEncryptionKeyMetadata) o;
        return Objects.equals(activeKeyId, that.activeKeyId) && Objects.equals(keys, that.keys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(activeKeyId, keys);
    }
}
