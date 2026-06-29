/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.chunk;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Stores the project encryption keys in project metadata.
 *
 * <p>PEK bytes are kept in plaintext in memory and over the TLS-protected transport channel. They are wrapped with a password (via
 * {@link PasswordBasedEncryption#wrap}) only when serialized to disk via {@link #toXContentChunked} in the
 * {@link Metadata.XContentContext#GATEWAY} context. The {@link PekEncryption} implementation is provided by {@link EncryptionPlugin} and
 * injected at deserialization time.
 *
 * <p>Keys are identified by a randomly generated key ID. The active key is identified by {@link #getActiveKeyId()}; its age drives the
 * next-rotation decision. Non-active keys age out after a grace period before being retired.
 *
 * <p>If a node cannot unwrap the on-disk blob at startup (missing or wrong password), {@link #fromXContent} produces a <em>degraded</em>
 * instance ({@link #isUnwrapFailed()} returns {@code true}) instead of throwing. The node starts normally, but the encryption service
 * reports {@code UNAVAILABLE_DECRYPTION_FAILED} and all encrypt/decrypt calls fail until the operator fixes the password and restarts,
 * or calls {@code POST /_encryption/_reset?accept_data_loss=true}.
 */
class ProjectEncryptionKeyMetadata extends AbstractNamedDiffable<Metadata.ProjectCustom> implements Metadata.ProjectCustom {
    static final String TYPE = "primary_encryption_key";

    private static final Logger logger = LogManager.getLogger(ProjectEncryptionKeyMetadata.class);

    @SuppressWarnings("unused")
    private static final TransportVersion PRIMARY_ENCRYPTION_KEY_VERSION = TransportVersion.fromName("primary_encryption_key");
    @SuppressWarnings("unused")
    private static final TransportVersion PRIMARY_ENCRYPTION_KEY_ROTATION = TransportVersion.fromName("primary_encryption_key_rotation");
    @SuppressWarnings("unused")
    private static final TransportVersion PRIMARY_ENCRYPTION_KEY_AT_REST = TransportVersion.fromName("primary_encryption_key_at_rest");

    static final TransportVersion PRIMARY_ENCRYPTION_KEY_CLEARTEXT_TRANSPORT = TransportVersion.fromName(
        "primary_encryption_key_cleartext_transport"
    );

    private static final ParseField KEYS_FIELD = new ParseField("keys");
    private static final ParseField ACTIVE_KEY_ID_FIELD = new ParseField("active_key_id");
    private static final ParseField PASSWORD_ID_FIELD = new ParseField("password_id");
    private static final ParseField BYTES_FIELD = new ParseField("bytes");
    private static final ParseField GENERATED_AT_FIELD = new ParseField("generated_at");
    private static final ParseField HANDLER_KEY_IDS_FIELD = new ParseField("handler_key_ids");

    /** Wraps and unwraps PEK bytes for disk (gateway) serialization. Injected by {@link EncryptionPlugin} at deserialization time. */
    interface PekEncryption {
        String activePasswordId();

        record WrappedKey(String passwordId, byte[] wrapped) {}

        WrappedKey wrap(byte[] plaintextPek);

        byte[] unwrap(byte[] wrappedPek, String passwordId);
    }

    /** Plaintext AES-256 key bytes and generation timestamp */
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

        @Override
        public String toString() {
            return "KeyEntry[generatedAt=" + generatedAt + "]";
        }
    }

    private final Map<String, KeyEntry> keys;
    private final String activeKeyId;
    private final String passwordId;
    private final Map<String, String> handlerKeyIds;
    private final PekEncryption pekEncryption;
    @Nullable
    private final String unwrapFailureReason;

    private final ConcurrentHashMap<String, PekEncryption.WrappedKey> wrappedKeyCache = new ConcurrentHashMap<>();

    private record GatewayBytes(String passwordId, Map<String, byte[]> wrappedByKeyId) {}

    private record PreservedGatewayBlob(
        String activeKeyId,
        String passwordId,
        Map<String, KeyEntry> wrappedKeys,
        Map<String, String> handlerKeyIds
    ) {}

    private record ParseContext(PekEncryption pek, DegradedBlobHolder holder) {}

    /**
     * Node-scoped holder for the wrapped PEK ciphertext last read from this node's own disk. Populated by
     * {@link #fromXContent} and consulted by degraded instances during GATEWAY serialization so each node
     * re-emits its own password-specific blob regardless of what a degraded master published.
     */
    static final class DegradedBlobHolder {
        volatile PreservedGatewayBlob blob;
    }

    @Nullable
    private final DegradedBlobHolder degradedBlobHolder;

    ProjectEncryptionKeyMetadata(
        Map<String, KeyEntry> keys,
        String activeKeyId,
        String passwordId,
        Map<String, String> handlerKeyIds,
        PekEncryption pekEncryption
    ) {
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
        this.pekEncryption = pekEncryption;
        this.unwrapFailureReason = null;
        this.degradedBlobHolder = null;
    }

    /**
     * Private constructor used only by {@link #degraded} — skips all invariant validation so the
     * degraded instance can carry diagnostic fields without usable key material.
     */
    private ProjectEncryptionKeyMetadata(
        Map<String, KeyEntry> keys,
        @Nullable String activeKeyId,
        String passwordId,
        Map<String, String> handlerKeyIds,
        PekEncryption pekEncryption,
        @Nullable String unwrapFailureReason,
        @Nullable DegradedBlobHolder degradedBlobHolder
    ) {
        this.keys = keys;
        this.activeKeyId = activeKeyId;
        this.passwordId = passwordId;
        this.handlerKeyIds = handlerKeyIds;
        this.pekEncryption = pekEncryption;
        this.unwrapFailureReason = unwrapFailureReason;
        this.degradedBlobHolder = degradedBlobHolder;
    }

    ProjectEncryptionKeyMetadata(StreamInput in, PekEncryption pekEncryption, DegradedBlobHolder degradedBlobHolder) throws IOException {
        this.pekEncryption = pekEncryption;
        this.degradedBlobHolder = degradedBlobHolder;
        if (in.getTransportVersion().supports(PRIMARY_ENCRYPTION_KEY_CLEARTEXT_TRANSPORT) && in.readBoolean()) {
            this.unwrapFailureReason = in.readString();
            this.activeKeyId = in.readString();
            this.passwordId = in.readString();
            this.keys = Map.of();
            this.handlerKeyIds = Map.of();
        } else {
            this.unwrapFailureReason = null;
            Map<String, KeyEntry> readKeys = in.readImmutableMap(StreamInput::readString, KeyEntry::new);
            String readActiveKeyId = in.readString();
            String readPasswordId = in.readString();
            Map<String, String> readHandlerKeyIds = in.readImmutableMap(StreamInput::readString, StreamInput::readString);
            if (readKeys.isEmpty()) {
                throw new IllegalArgumentException("Keys map must not be empty");
            }
            if (readKeys.containsKey(readActiveKeyId) == false) {
                throw new IllegalArgumentException("Active key ID [" + readActiveKeyId + "] not found in keys");
            }
            if (readHandlerKeyIds.values().stream().allMatch(readKeys::containsKey) == false) {
                throw new IllegalArgumentException("handlerKeyIds references key IDs absent from keys");
            }
            this.keys = readKeys;
            this.activeKeyId = readActiveKeyId;
            this.passwordId = readPasswordId;
            this.handlerKeyIds = readHandlerKeyIds;
        }
    }

    /**
     * Creates a degraded instance that carries the failure reason but no usable key material. Used by {@link #fromXContent} when the
     * on-disk blob cannot be unwrapped. Holds the original ciphertext from disk, or {@code null} if the disk blob had no key material.
     * When non-null, {@link #toXContentChunked} in GATEWAY context re-emits these bytes verbatim so recovery remains possible once the
     * correct password is supplied.
     */
    static ProjectEncryptionKeyMetadata degraded(
        @Nullable String activeKeyId,
        String passwordId,
        PekEncryption pekEncryption,
        String unwrapFailureReason,
        @Nullable DegradedBlobHolder degradedBlobHolder
    ) {
        return new ProjectEncryptionKeyMetadata(
            Map.of(),
            activeKeyId,
            passwordId,
            Map.of(),
            pekEncryption,
            unwrapFailureReason,
            degradedBlobHolder
        );
    }

    static String generateKeyId() {
        return UUIDs.randomBase64UUID();
    }

    /**
     * Returns {@code true} if this instance was created because {@link #fromXContent} could not unwrap
     * the on-disk key blob. When {@code true}, {@link #getKeys()} is empty and the encryption service
     * reports {@code UNAVAILABLE_DECRYPTION_FAILED}.
     */
    boolean isUnwrapFailed() {
        return unwrapFailureReason != null;
    }

    @Nullable
    String getActiveKeyId() {
        return activeKeyId;
    }

    String getPasswordId() {
        return passwordId;
    }

    Map<String, KeyEntry> getKeys() {
        return keys;
    }

    long getGeneratedAt(String keyId) {
        KeyEntry entry = keys.get(keyId);
        return entry != null ? entry.generatedAt() : 0L;
    }

    Map<String, String> getHandlerKeyIds() {
        return handlerKeyIds;
    }

    boolean isHandlerOnActive(String customName) {
        return activeKeyId != null && activeKeyId.equals(handlerKeyIds.get(customName));
    }

    ProjectEncryptionKeyMetadata withHandlerKeyId(String customName, String keyId) {
        Map<String, String> updated = new HashMap<>(handlerKeyIds);
        updated.put(customName, keyId);
        return new ProjectEncryptionKeyMetadata(keys, activeKeyId, passwordId, updated, pekEncryption);
    }

    /**
     * Returns non-active key IDs whose deactivation time (the {@code generatedAt} of the key that replaced them) is strictly less than
     * {@code cutoffDeactivationMillis} and that are not still referenced by {@link #getHandlerKeyIds()}.
     */
    Set<String> findRetireableKeyIds(long cutoffDeactivationMillis) {
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
        return PRIMARY_ENCRYPTION_KEY_CLEARTEXT_TRANSPORT;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(PRIMARY_ENCRYPTION_KEY_CLEARTEXT_TRANSPORT)) {
            out.writeBoolean(unwrapFailureReason != null);
            if (unwrapFailureReason != null) {
                out.writeString(unwrapFailureReason);
                out.writeString(activeKeyId != null ? activeKeyId : "");
                out.writeString(passwordId);
                return;
            }
        }
        out.writeMap(keys, StreamOutput::writeString, (o, e) -> e.writeTo(o));
        out.writeString(activeKeyId);
        out.writeString(passwordId);
        out.writeMap(handlerKeyIds, StreamOutput::writeString, StreamOutput::writeString);
    }

    static NamedDiff<Metadata.ProjectCustom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Metadata.ProjectCustom.class, TYPE, in);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        if (Metadata.XContentContext.from(params) != Metadata.XContentContext.GATEWAY) {
            return apiChunk();
        }

        if (unwrapFailureReason != null) {
            PreservedGatewayBlob blob = degradedBlobHolder != null ? degradedBlobHolder.blob : null;
            if (blob == null || blob.wrappedKeys().isEmpty()) {
                return emptyGatewayChunk();
            }
            return gatewayChunk(blob.activeKeyId(), blob.passwordId(), blob.wrappedKeys(), blob.handlerKeyIds());
        }

        GatewayBytes gw = getOrComputeWrappedKeys();
        Map<String, KeyEntry> wrappedEntries = HashMap.newHashMap(keys.size());
        for (Map.Entry<String, KeyEntry> e : keys.entrySet()) {
            wrappedEntries.put(e.getKey(), new KeyEntry(gw.wrappedByKeyId().get(e.getKey()), e.getValue().generatedAt()));
        }
        return gatewayChunk(activeKeyId, gw.passwordId(), wrappedEntries, handlerKeyIds);
    }

    private Iterator<? extends ToXContent> apiChunk() {
        return chunk((builder, p) -> {
            builder.field(ACTIVE_KEY_ID_FIELD.getPreferredName(), activeKeyId);
            builder.field(PASSWORD_ID_FIELD.getPreferredName(), passwordId);
            builder.startObject(KEYS_FIELD.getPreferredName());
            for (Map.Entry<String, KeyEntry> entry : keys.entrySet()) {
                builder.startObject(entry.getKey());
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

    private Iterator<? extends ToXContent> gatewayChunk(
        String activeKeyId,
        String passwordId,
        Map<String, KeyEntry> wrappedEntries,
        Map<String, String> handlerIds
    ) {
        return chunk((builder, p) -> {
            builder.field(ACTIVE_KEY_ID_FIELD.getPreferredName(), activeKeyId);
            builder.field(PASSWORD_ID_FIELD.getPreferredName(), passwordId);
            builder.startObject(KEYS_FIELD.getPreferredName());
            for (Map.Entry<String, KeyEntry> entry : wrappedEntries.entrySet()) {
                builder.startObject(entry.getKey());
                builder.field(BYTES_FIELD.getPreferredName(), entry.getValue().bytes());
                builder.field(GENERATED_AT_FIELD.getPreferredName(), entry.getValue().generatedAt());
                builder.endObject();
            }
            builder.endObject();
            builder.startObject(HANDLER_KEY_IDS_FIELD.getPreferredName());
            for (Map.Entry<String, String> entry : handlerIds.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
            return builder;
        });
    }

    private Iterator<? extends ToXContent> emptyGatewayChunk() {
        return chunk((builder, p) -> {
            builder.field(ACTIVE_KEY_ID_FIELD.getPreferredName(), activeKeyId != null ? activeKeyId : "");
            builder.field(PASSWORD_ID_FIELD.getPreferredName(), passwordId);
            builder.startObject(KEYS_FIELD.getPreferredName()).endObject();
            builder.startObject(HANDLER_KEY_IDS_FIELD.getPreferredName()).endObject();
            return builder;
        });
    }

    private GatewayBytes getOrComputeWrappedKeys() {
        String activeId = pekEncryption.activePasswordId();
        Map<String, byte[]> result = HashMap.newHashMap(keys.size());
        for (Map.Entry<String, KeyEntry> e : keys.entrySet()) {
            String keyId = e.getKey();
            PekEncryption.WrappedKey cached = wrappedKeyCache.get(keyId);
            if (cached == null || cached.passwordId().equals(activeId) == false) {
                PekEncryption.WrappedKey wk = pekEncryption.wrap(e.getValue().bytes());
                wrappedKeyCache.put(keyId, wk);
                result.put(keyId, wk.wrapped());
                activeId = wk.passwordId();
            } else {
                result.put(keyId, cached.wrapped());
            }
        }
        return new GatewayBytes(activeId, result);
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
    private static final ConstructingObjectParser<ProjectEncryptionKeyMetadata, ParseContext> PARSER = new ConstructingObjectParser<>(
        TYPE,
        false,
        (args, ctx) -> {
            String activeKeyId = (String) args[0];
            // password_id is optional for gateway BWC. Default to the empty string
            String passwordId = args[1] != null ? (String) args[1] : "";
            List<Tuple<String, KeyEntry>> list = (List<Tuple<String, KeyEntry>>) args[2];
            Map<String, KeyEntry> keys = list.stream().collect(Collectors.toMap(Tuple::v1, Tuple::v2));
            Map<String, String> handlerKeyIds = args[3] != null ? (Map<String, String>) args[3] : Map.of();
            if (keys.isEmpty()) {
                return degraded(
                    activeKeyId,
                    passwordId,
                    ctx.pek(),
                    "no key material found on disk (previously degraded state)",
                    ctx.holder()
                );
            }
            return new ProjectEncryptionKeyMetadata(keys, activeKeyId, passwordId, handlerKeyIds, ctx.pek());
        }
    );
    static {
        PARSER.declareString(constructorArg(), ACTIVE_KEY_ID_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), PASSWORD_ID_FIELD);
        PARSER.declareNamedObjects(constructorArg(), (p, c, name) -> Tuple.tuple(name, KEY_ENTRY_PARSER.apply(p, name)), KEYS_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.mapStrings(), HANDLER_KEY_IDS_FIELD);
    }

    static ProjectEncryptionKeyMetadata fromXContent(
        XContentParser parser,
        PekEncryption pekEncryption,
        DegradedBlobHolder degradedBlobHolder
    ) throws IOException {
        ProjectEncryptionKeyMetadata parsed = PARSER.parse(parser, new ParseContext(pekEncryption, degradedBlobHolder));
        if (parsed.isUnwrapFailed()) {
            return parsed;
        }
        Map<String, KeyEntry> plaintextKeys = HashMap.newHashMap(parsed.keys.size());
        try {
            for (Map.Entry<String, KeyEntry> entry : parsed.keys.entrySet()) {
                byte[] plaintext = pekEncryption.unwrap(entry.getValue().bytes(), parsed.passwordId);
                plaintextKeys.put(entry.getKey(), new KeyEntry(plaintext, entry.getValue().generatedAt()));
            }
        } catch (RuntimeException e) {
            logger.error(
                () -> Strings.format(
                    "failed to unwrap project encryption key [passwordId=%s] from disk; node starting in degraded state."
                        + " To recover: fix the password and restart, or call POST /_encryption/_reset?accept_data_loss=true",
                    parsed.passwordId
                ),
                e
            );
            degradedBlobHolder.blob = new PreservedGatewayBlob(parsed.activeKeyId, parsed.passwordId, parsed.keys, parsed.handlerKeyIds);
            return degraded(parsed.activeKeyId, parsed.passwordId, pekEncryption, e.getMessage(), degradedBlobHolder);
        }
        degradedBlobHolder.blob = new PreservedGatewayBlob(parsed.activeKeyId, parsed.passwordId, parsed.keys, parsed.handlerKeyIds);
        return new ProjectEncryptionKeyMetadata(
            Map.copyOf(plaintextKeys),
            parsed.activeKeyId,
            parsed.passwordId,
            parsed.handlerKeyIds,
            pekEncryption
        );
    }

    @Override
    public String toString() {
        if (unwrapFailureReason != null) {
            return "ProjectEncryptionKeyMetadata{DEGRADED, activeKeyId="
                + activeKeyId
                + ", passwordId="
                + passwordId
                + ", reason="
                + unwrapFailureReason
                + "}";
        }
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
            && Objects.equals(handlerKeyIds, that.handlerKeyIds)
            && Objects.equals(unwrapFailureReason, that.unwrapFailureReason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(activeKeyId, passwordId, keys, handlerKeyIds, unwrapFailureReason);
    }
}
