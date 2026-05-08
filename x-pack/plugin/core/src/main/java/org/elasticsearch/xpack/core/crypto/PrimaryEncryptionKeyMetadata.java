/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.crypto;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.chunk;

/**
 * Stores the primary encryption keys in project metadata.
 *
 * <p>The primary encryption key (PEK) is a randomly generated AES-256 key used to encrypt secrets stored in cluster state. The plaintext
 * PEK is held in-memory and distributed to all nodes via cluster state updates over the transport layer.
 *
 * <p>Keys are identified by a randomly generated key ID. Multiple keys are retained to support key rotation. The active key (used for new
 * encrypt operations) is explicitly identified by {@link #getActiveKeyId()}. Previous keys are kept so that data encrypted with older keys
 * can still be decrypted, until rotation completes and they are retired.
 *
 * <p>Rotation tracking lives alongside the keys: {@link #getRotationState()} reflects whether a rotation is in progress, and
 * {@link #getLastRotatedMillis()} is the time of the most recent successful rotation (or initial install).
 *
 * <p>This metadata is persisted to disk via the gateway ({@link Metadata.XContentContext#GATEWAY}) but is NOT exposed in REST APIs or
 * included in snapshots.
 */
public class PrimaryEncryptionKeyMetadata extends AbstractNamedDiffable<Metadata.ProjectCustom> implements Metadata.ProjectCustom {

    public static final String TYPE = "primary_encryption_key";

    public static final TransportVersion PRIMARY_ENCRYPTION_KEY_VERSION = TransportVersion.fromName("primary_encryption_key");
    public static final TransportVersion PRIMARY_ENCRYPTION_KEY_ROTATION = TransportVersion.fromName("primary_encryption_key_rotation");

    /**
     * Lifecycle state of the keys held in this metadata.
     */
    public enum RotationState {
        /** No rotation in progress: all data is encrypted under {@link #getActiveKeyId()}. */
        STABLE,
        /** A new active key has been published; registered handlers are re-encrypting their data. */
        ROTATING
    }

    private static final int KEY_LENGTH_BYTES = 32;
    private static final String KEY_ALGORITHM = "AES";
    private static final ParseField KEYS_FIELD = new ParseField("keys");
    private static final ParseField ACTIVE_KEY_ID_FIELD = new ParseField("active_key_id");
    private static final ParseField LAST_ROTATED_MILLIS_FIELD = new ParseField("last_rotated_millis");
    private static final ParseField ROTATION_STATE_FIELD = new ParseField("rotation_state");

    private final Map<String, byte[]> keys;
    private final String activeKeyId;
    private final long lastRotatedMillis;
    private final RotationState rotationState;

    public PrimaryEncryptionKeyMetadata(Map<String, byte[]> keys, String activeKeyId) {
        this(keys, activeKeyId, 0L, RotationState.STABLE);
    }

    public PrimaryEncryptionKeyMetadata(Map<String, byte[]> keys, String activeKeyId, long lastRotatedMillis, RotationState rotationState) {
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("Keys map must not be empty");
        }
        if (keys.containsKey(activeKeyId) == false) {
            throw new IllegalArgumentException("Active key ID [" + activeKeyId + "] not found in keys");
        }
        for (Map.Entry<String, byte[]> entry : keys.entrySet()) {
            if (entry.getValue().length != KEY_LENGTH_BYTES) {
                throw new IllegalArgumentException(
                    "Key [" + entry.getKey() + "] must be " + KEY_LENGTH_BYTES + " bytes, got " + entry.getValue().length
                );
            }
        }
        this.keys = keys;
        this.activeKeyId = activeKeyId;
        this.lastRotatedMillis = lastRotatedMillis;
        this.rotationState = Objects.requireNonNull(rotationState, "rotationState");
    }

    public PrimaryEncryptionKeyMetadata(StreamInput in) throws IOException {
        this(
            in.readImmutableMap(StreamInput::readString, StreamInput::readByteArray),
            in.readString(),
            in.getTransportVersion().supports(PRIMARY_ENCRYPTION_KEY_ROTATION) ? in.readVLong() : 0L,
            in.getTransportVersion().supports(PRIMARY_ENCRYPTION_KEY_ROTATION) ? in.readEnum(RotationState.class) : RotationState.STABLE
        );
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
     * Returns a defensive copy of the raw key bytes for the specified key ID,
     * or {@code null} if the key ID is not present.
     */
    public byte[] getKeyBytes(String keyId) {
        byte[] keyBytes = keys.get(keyId);
        return keyBytes != null ? keyBytes.clone() : null;
    }

    /**
     * Returns the active key as a {@link SecretKey} suitable for use with AES cryptographic operations.
     */
    public SecretKey toSecretKey() {
        return new SecretKeySpec(keys.get(activeKeyId), KEY_ALGORITHM);
    }

    /**
     * Returns the key for the specified key ID as a {@link SecretKey},
     * or {@code null} if the key ID is not present.
     */
    public SecretKey toSecretKey(String keyId) {
        byte[] keyBytes = keys.get(keyId);
        return keyBytes != null ? new SecretKeySpec(keyBytes, KEY_ALGORITHM) : null;
    }

    /**
     * Returns an unmodifiable view of the keys in this metadata.
     */
    public Map<String, byte[]> getKeys() {
        return Collections.unmodifiableMap(keys);
    }

    /**
     * Time of the most recent successful rotation, or of initial install if no rotation has completed yet.
     */
    public long getLastRotatedMillis() {
        return lastRotatedMillis;
    }

    /**
     * Returns the current rotation lifecycle state.
     */
    public RotationState getRotationState() {
        return rotationState;
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
        out.writeMap(keys, StreamOutput::writeString, StreamOutput::writeByteArray);
        out.writeString(activeKeyId);
        if (out.getTransportVersion().supports(PRIMARY_ENCRYPTION_KEY_ROTATION)) {
            out.writeVLong(lastRotatedMillis);
            out.writeEnum(rotationState);
        }
    }

    public static NamedDiff<Metadata.ProjectCustom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Metadata.ProjectCustom.class, TYPE, in);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return chunk((builder, params) -> {
            builder.field(ACTIVE_KEY_ID_FIELD.getPreferredName(), activeKeyId);
            builder.field(LAST_ROTATED_MILLIS_FIELD.getPreferredName(), lastRotatedMillis);
            builder.field(ROTATION_STATE_FIELD.getPreferredName(), rotationState.name());
            builder.startObject(KEYS_FIELD.getPreferredName());
            for (Map.Entry<String, byte[]> entry : keys.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
            return builder;
        });
    }

    public static Metadata.ProjectCustom fromXContent(XContentParser parser) throws IOException {
        String activeKeyId = null;
        Map<String, byte[]> keys = Map.of();
        long lastRotatedMillis = 0L;
        RotationState rotationState = RotationState.STABLE;
        String currentFieldName;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
                if (ACTIVE_KEY_ID_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    parser.nextToken();
                    activeKeyId = parser.text();
                } else if (LAST_ROTATED_MILLIS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    parser.nextToken();
                    lastRotatedMillis = parser.longValue();
                } else if (ROTATION_STATE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    parser.nextToken();
                    rotationState = RotationState.valueOf(parser.text().toUpperCase(Locale.ROOT));
                } else if (KEYS_FIELD.match(currentFieldName, parser.getDeprecationHandler())
                    && parser.nextToken() == XContentParser.Token.START_OBJECT) {
                        keys = new HashMap<>();
                        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                            String keyId = parser.currentName();
                            parser.nextToken();
                            keys.put(keyId, parser.binaryValue());
                        }
                    }
            }
        }
        if (activeKeyId == null) {
            throw new IllegalArgumentException("Missing required field [" + ACTIVE_KEY_ID_FIELD.getPreferredName() + "]");
        }
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("Missing required field [" + KEYS_FIELD.getPreferredName() + "]");
        }
        return new PrimaryEncryptionKeyMetadata(keys, activeKeyId, lastRotatedMillis, rotationState);
    }

    @Override
    public String toString() {
        return "PrimaryEncryptionKeyMetadata{activeKeyId="
            + activeKeyId
            + ", keyCount="
            + keys.size()
            + ", lastRotatedMillis="
            + lastRotatedMillis
            + ", rotationState="
            + rotationState
            + ", [keys secret]}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PrimaryEncryptionKeyMetadata that = (PrimaryEncryptionKeyMetadata) o;
        if (Objects.equals(activeKeyId, that.activeKeyId) == false) return false;
        if (lastRotatedMillis != that.lastRotatedMillis) return false;
        if (rotationState != that.rotationState) return false;
        if (keys.size() != that.keys.size()) return false;
        for (Map.Entry<String, byte[]> entry : keys.entrySet()) {
            if (Arrays.equals(entry.getValue(), that.keys.get(entry.getKey())) == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            activeKeyId,
            lastRotatedMillis,
            rotationState,
            keys.keySet(),
            keys.values().stream().mapToInt(Arrays::hashCode).sum()
        );
    }
}
