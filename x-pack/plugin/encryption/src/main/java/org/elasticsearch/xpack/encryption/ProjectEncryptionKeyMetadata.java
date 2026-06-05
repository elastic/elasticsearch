/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

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
import org.elasticsearch.xpack.encryption.spi.EncryptedData;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.chunk;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Stores the project encryption keys in project metadata.
 *
 * <p>The project encryption key (PEK) is a randomly generated AES-256 key used to encrypt secrets stored in cluster state. Under the
 * current at-rest layout, the PEK is never persisted in plaintext: each {@link KeyEntry#bytes()} value is the output of
 * {@link PasswordBasedEncryption#wrap(byte[], String, char[])} — a salted, password-wrapped form of the raw key. The matching password
 * lives in secure settings under {@code cluster.state.encryption.password.<id>}, where {@code <id>} is the metadata-level
 * {@link #getPasswordId()}.
 *
 * <p>Keys are identified by a randomly generated key ID. Multiple keys are retained to support key rotation. The active key is explicitly
 * identified by {@link #getActiveKeyId()}. Each key carries a {@link KeyEntry#generatedAt()} timestamp. The active key's age drives the
 * next-rotation decision; non-active keys age out independently after a grace period before being retired.
 *
 */
class ProjectEncryptionKeyMetadata extends AbstractNamedDiffable<Metadata.ProjectCustom> implements Metadata.ProjectCustom {
    static final String TYPE = "primary_encryption_key";

    static final TransportVersion PRIMARY_ENCRYPTION_KEY_VERSION = TransportVersion.fromName("primary_encryption_key");
    static final TransportVersion PRIMARY_ENCRYPTION_KEY_ROTATION = TransportVersion.fromName("primary_encryption_key_rotation");
    static final TransportVersion PRIMARY_ENCRYPTION_KEY_AT_REST = TransportVersion.fromName("primary_encryption_key_at_rest");

    private static final ParseField KEYS_FIELD = new ParseField("keys");
    private static final ParseField ACTIVE_KEY_ID_FIELD = new ParseField("active_key_id");
    private static final ParseField PASSWORD_ID_FIELD = new ParseField("password_id");
    private static final ParseField BYTES_FIELD = new ParseField("bytes");
    private static final ParseField GENERATED_AT_FIELD = new ParseField("generated_at");
    private static final ParseField HANDLER_KEY_IDS_FIELD = new ParseField("handler_key_ids");

    /**
     * A single key entry: the password-wrapped key bytes plus the timestamp at which the underlying plaintext PEK was generated.
     *
     * <p>{@link #bytes()} is the {@link EncryptedData#payload()} produced by
     * {@link PasswordBasedEncryption#wrap(byte[], String, char[])}. Construct the full {@link EncryptedData} by pairing it with
     * {@link ProjectEncryptionKeyMetadata#getPasswordId()}.
     */
    record KeyEntry(byte[] bytes, long generatedAt) implements Writeable {

        KeyEntry {
            Objects.requireNonNull(bytes, "bytes");
        }

        KeyEntry(StreamInput in) throws IOException {
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
    private final String passwordId;
    private final Map<String, String> handlerKeyIds;

    ProjectEncryptionKeyMetadata(Map<String, KeyEntry> keys, String activeKeyId, String passwordId) {
        this(keys, activeKeyId, passwordId, Map.of());
    }

    /**
     * @param keys          map of key id -> wrapped key entry; must be non-empty.
     * @param activeKeyId   id of the active key; must be present in {@code keys} and have the {@link KeyEntry#generatedAt() newest}
     *                      timestamp — {@link #findRetireableKeyIds} relies on this ordering invariant.
     * @param passwordId    id of the password the entries are wrapped under; must be non-null. The empty string is reserved as a
     *                      placeholder for pre-at-rest gateway/wire BWC and is replaced by the rotation coordinator on the next tick.
     * @param handlerKeyIds map of handler {@code customName} -> the key id its data is currently encrypted under; values must be
     *                      present in {@code keys}.
     */
    ProjectEncryptionKeyMetadata(Map<String, KeyEntry> keys, String activeKeyId, String passwordId, Map<String, String> handlerKeyIds) {
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("Keys map must not be empty");
        }
        if (keys.containsKey(activeKeyId) == false) {
            throw new IllegalArgumentException("Active key ID [" + activeKeyId + "] not found in keys");
        }
        Objects.requireNonNull(passwordId, "passwordId");
        long maxGeneratedAt = keys.values().stream().mapToLong(KeyEntry::generatedAt).max().getAsLong();
        if (maxGeneratedAt != keys.get(activeKeyId).generatedAt()) {
            throw new IllegalArgumentException("Active key [" + activeKeyId + "] must be the newest entry by generatedAt in " + keys);
        }
        if (handlerKeyIds.values().stream().allMatch(keys::containsKey) == false) {
            throw new IllegalArgumentException("handlerKeyIds " + handlerKeyIds + " references key IDs absent from keys " + keys.keySet());
        }
        this.keys = Map.copyOf(keys);
        this.activeKeyId = activeKeyId;
        this.passwordId = passwordId;
        this.handlerKeyIds = Map.copyOf(handlerKeyIds);
    }

    ProjectEncryptionKeyMetadata(StreamInput in) throws IOException {
        this(readKeysFrom(in), in.readString(), readPasswordIdFrom(in), readHandlerKeyIdsFrom(in));
    }

    private static Map<String, KeyEntry> readKeysFrom(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(PRIMARY_ENCRYPTION_KEY_ROTATION)) {
            return in.readImmutableMap(StreamInput::readString, KeyEntry::new);
        }
        // Pre-rotation: KeyEntry was just bytes; synthesize generatedAt=0 (the rotation coordinator replaces these on the next cycle).
        return in.readImmutableMap(StreamInput::readString, streamInput -> new KeyEntry(streamInput.readByteArray(), 0L));
    }

    private static String readPasswordIdFrom(StreamInput in) throws IOException {
        // passwordId was added in PRIMARY_ENCRYPTION_KEY_AT_REST. Older streams don't carry it; default to the empty string
        if (in.getTransportVersion().supports(PRIMARY_ENCRYPTION_KEY_AT_REST)) {
            return in.readString();
        }
        return "";
    }

    private static Map<String, String> readHandlerKeyIdsFrom(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(PRIMARY_ENCRYPTION_KEY_ROTATION)) {
            return in.readImmutableMap(StreamInput::readString, StreamInput::readString);
        }
        return Map.of();
    }

    /**
     * Generates a random key ID.
     */
    static String generateKeyId() {
        return UUIDs.randomBase64UUID();
    }

    /**
     * Returns the active key's ID.
     */
    String getActiveKeyId() {
        return activeKeyId;
    }

    /**
     * Returns the id of the password under which every entry's {@link KeyEntry#bytes()} is wrapped.
     */
    String getPasswordId() {
        return passwordId;
    }

    /**
     * Returns the wrapped (password-encrypted) PEK for {@code keyId} as an {@link EncryptedData} suitable to pass to
     * {@link PasswordBasedEncryption#unwrap(EncryptedData, char[])}, or {@code null} if the key ID is not present.
     */
    EncryptedData getEncryptedKey(String keyId) {
        KeyEntry entry = keys.get(keyId);
        return entry != null ? new EncryptedData(passwordId, entry.bytes()) : null;
    }

    /**
     * Returns an unmodifiable view of the keys (wrapped key bytes + generation timestamps) keyed by key ID.
     */
    Map<String, KeyEntry> getKeys() {
        return keys;
    }

    /**
     * Returns the time at which the specified key was generated, or {@code 0L} if the key ID is not present.
     */
    long getGeneratedAt(String keyId) {
        KeyEntry entry = keys.get(keyId);
        return entry != null ? entry.generatedAt() : 0L;
    }

    /**
     * Returns an unmodifiable map of handler {@code customName} -> the key ID that handler's data is currently encrypted under.
     */
    Map<String, String> getHandlerKeyIds() {
        return handlerKeyIds;
    }

    /**
     * Returns {@code true} when the handler's data has been re-encrypted under the current active key.
     */
    boolean isHandlerOnActive(String customName) {
        return activeKeyId.equals(handlerKeyIds.get(customName));
    }

    /**
     * Returns a copy of this metadata with {@code handlerKeyIds[customName] = keyId}.
     */
    ProjectEncryptionKeyMetadata withHandlerKeyId(String customName, String keyId) {
        Map<String, String> updated = new HashMap<>(handlerKeyIds);
        updated.put(customName, keyId);
        return new ProjectEncryptionKeyMetadata(keys, activeKeyId, passwordId, updated);
    }

    /**
     * Returns the IDs of non-active keys that have been deactivated for long enough to retire.
     *
     * <p>A non-active key's "deactivation time" is the {@code generatedAt} of the next-newer key in the map (the key that replaced it).
     * The grace period is measured from that deactivation time.
     *
     * <p>A key is retire-eligible iff its deactivation time is strictly less than {@code cutoffDeactivationMillis} AND it is not still
     * referenced by any entry in {@link #getHandlerKeyIds()}
     */
    Set<String> findRetireableKeyIds(long cutoffDeactivationMillis) {
        // Sort by generatedAt ascending so each entry's deactivation time is the next entry's generatedAt.
        // The active key is always the newest entry, so it lands last and never has its successor inspected.
        List<Map.Entry<String, KeyEntry>> sorted = keys.entrySet()
            .stream()
            .sorted(Comparator.comparingLong(e -> e.getValue().generatedAt()))
            .toList();
        Set<String> stillReferenced = Set.copyOf(handlerKeyIds.values());
        Set<String> retireable = new HashSet<>();
        for (int i = 0; i < sorted.size() - 1; i++) {
            String id = sorted.get(i).getKey();
            if (id.equals(activeKeyId) || stillReferenced.contains(id)) {
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
        return EnumSet.of(Metadata.XContentContext.GATEWAY, Metadata.XContentContext.API);
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
        out.writeMap(keys, StreamOutput::writeString, (o, e) -> e.writeTo(o));
        out.writeString(activeKeyId);
        if (out.getTransportVersion().supports(PRIMARY_ENCRYPTION_KEY_AT_REST)) {
            out.writeString(passwordId);
        }
        out.writeMap(handlerKeyIds, StreamOutput::writeString, StreamOutput::writeString);
    }

    static NamedDiff<Metadata.ProjectCustom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Metadata.ProjectCustom.class, TYPE, in);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        // Redact wrapped key bytes when rendering for the cluster-state REST API. Gateway (and any unspecified context, which
        // defaults to API) callers that need the bytes for disk round-trip must use the GATEWAY context, where bytes are included.
        final boolean redactWrappedBytes = Metadata.XContentContext.from(params) == Metadata.XContentContext.API;
        return chunk((builder, p) -> {
            builder.field(ACTIVE_KEY_ID_FIELD.getPreferredName(), activeKeyId);
            builder.field(PASSWORD_ID_FIELD.getPreferredName(), passwordId);
            builder.startObject(KEYS_FIELD.getPreferredName());
            for (Map.Entry<String, KeyEntry> entry : keys.entrySet()) {
                builder.startObject(entry.getKey());
                if (redactWrappedBytes == false) {
                    builder.field(BYTES_FIELD.getPreferredName(), entry.getValue().bytes());
                }
                builder.field(GENERATED_AT_FIELD.getPreferredName(), entry.getValue().generatedAt());
                builder.endObject();
            }
            builder.endObject();
            builder.startObject(HANDLER_KEY_IDS_FIELD.getPreferredName());
            for (Map.Entry<String, String> entry : handlerKeyIds.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
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
    private static final ConstructingObjectParser<ProjectEncryptionKeyMetadata, Void> PARSER = new ConstructingObjectParser<>(
        TYPE,
        false,
        args -> {
            String activeKeyId = (String) args[0];
            // password_id is optional for gateway BWC. Default to the empty string
            String passwordId = args[1] != null ? (String) args[1] : "";
            List<Tuple<String, KeyEntry>> list = (List<Tuple<String, KeyEntry>>) args[2];
            Map<String, KeyEntry> keys = list.stream().collect(Collectors.toMap(Tuple::v1, Tuple::v2));
            Map<String, String> handlerKeyIds = args[3] != null ? (Map<String, String>) args[3] : Map.of();
            return new ProjectEncryptionKeyMetadata(keys, activeKeyId, passwordId, handlerKeyIds);
        }
    );
    static {
        PARSER.declareString(constructorArg(), ACTIVE_KEY_ID_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), PASSWORD_ID_FIELD);
        PARSER.declareNamedObjects(constructorArg(), (p, c, name) -> Tuple.tuple(name, KEY_ENTRY_PARSER.apply(p, name)), KEYS_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.mapStrings(), HANDLER_KEY_IDS_FIELD);
    }

    static Metadata.ProjectCustom fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public String toString() {
        StringBuilder ts = new StringBuilder();
        for (String id : new TreeSet<>(keys.keySet())) {
            if (ts.isEmpty() == false) {
                ts.append(", ");
            }
            ts.append(id).append('=').append(keys.get(id).generatedAt());
        }
        return "ProjectEncryptionKeyMetadata{activeKeyId="
            + activeKeyId
            + ", passwordId="
            + passwordId
            + ", keyCount="
            + keys.size()
            + ", generatedAt="
            + ts
            + ", handlerKeyIds="
            + new TreeSet<>(handlerKeyIds.keySet())
            + ", [keys secret]}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProjectEncryptionKeyMetadata that = (ProjectEncryptionKeyMetadata) o;
        return Objects.equals(activeKeyId, that.activeKeyId)
            && Objects.equals(passwordId, that.passwordId)
            && Objects.equals(keys, that.keys)
            && Objects.equals(handlerKeyIds, that.handlerKeyIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(activeKeyId, passwordId, keys, handlerKeyIds);
    }
}
