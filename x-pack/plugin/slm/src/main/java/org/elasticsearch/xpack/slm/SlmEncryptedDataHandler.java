/*
* Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
* or more contributor license agreements. Licensed under the Elastic License
* 2.0; you may not use this file except in compliance with the Elastic License
* 2.0.
*/

package org.elasticsearch.xpack.slm;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.slm.SnapshotInvocationRecord;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotRetentionConfiguration;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Handles PEK rotation and snapshot export/import for SLM policy encryption passwords.
 * Each {@link SnapshotLifecyclePolicy} may carry an {@link EncryptedData} password field encrypted
 * under the cluster PEK; this handler re-encrypts those fields on key rotation.
 */
public final class SlmEncryptedDataHandler implements EncryptedDataHandler<SnapshotLifecycleMetadata> {

    private static final int FORMAT_VERSION = 1;

    @Override
    public String customName() {
        return SnapshotLifecycleMetadata.TYPE;
    }

    @Override
    @SuppressWarnings("deprecation")
    public SnapshotLifecycleMetadata reEncrypt(SnapshotLifecycleMetadata current, EncryptionService encryptionService, String activeKeyId) {
        if (current == null) {
            return null;
        }
        Map<String, SnapshotLifecyclePolicyMetadata> rebuilt = new HashMap<>(current.getSnapshotConfigurations());
        boolean changed = false;
        for (Map.Entry<String, SnapshotLifecyclePolicyMetadata> entry : current.getSnapshotConfigurations().entrySet()) {
            SnapshotLifecyclePolicyMetadata policyMeta = entry.getValue();
            EncryptedData existing = policyMeta.getPolicy().getEncryptedPassword();
            if (existing == null || existing.keyId().equals(activeKeyId)) {
                continue;
            }
            byte[] plaintext = encryptionService.decrypt(existing);
            try {
                EncryptedData reEncrypted = encryptionService.encrypt(plaintext);
                SnapshotLifecyclePolicy newPolicy = policyMeta.getPolicy().withEncryptedPassword(reEncrypted);
                rebuilt.put(entry.getKey(), SnapshotLifecyclePolicyMetadata.builder(policyMeta).setPolicy(newPolicy).build());
                changed = true;
            } finally {
                Arrays.fill(plaintext, (byte) 0);
            }
        }
        return changed ? new SnapshotLifecycleMetadata(rebuilt, current.getOperationMode(), current.getStats()) : current;
    }

    /**
     * On destructive reset, wipe the encryption passwords from all SLM policies while preserving
     * the rest of each policy's configuration. The policies remain functional but will no longer
     * use a password when creating snapshots.
     */
    @Override
    @SuppressWarnings("deprecation")
    public SnapshotLifecycleMetadata onDestructiveReset(SnapshotLifecycleMetadata current) {
        if (current == null) {
            return null;
        }
        boolean changed = false;
        Map<String, SnapshotLifecyclePolicyMetadata> rebuilt = new HashMap<>(current.getSnapshotConfigurations());
        for (Map.Entry<String, SnapshotLifecyclePolicyMetadata> entry : current.getSnapshotConfigurations().entrySet()) {
            if (entry.getValue().getPolicy().getEncryptedPassword() != null) {
                SnapshotLifecyclePolicy cleared = entry.getValue().getPolicy().withEncryptedPassword(null);
                rebuilt.put(entry.getKey(), SnapshotLifecyclePolicyMetadata.builder(entry.getValue()).setPolicy(cleared).build());
                changed = true;
            }
        }
        return changed ? new SnapshotLifecycleMetadata(rebuilt, current.getOperationMode(), current.getStats()) : current;
    }

    /**
     * Serializes the complete {@link SnapshotLifecycleMetadata} with each policy's encryption password
     * decrypted to plaintext bytes, so the snapshot blob can protect it with the user-supplied password.
     * Wire format: vint FORMAT_VERSION, vint policyCount, then per-policy fields (see {@link #writePolicy}).
     */
    @Override
    public byte[] toSnapshotBytes(SnapshotLifecycleMetadata current, EncryptionService encryptionService) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(FORMAT_VERSION);
            Map<String, SnapshotLifecyclePolicyMetadata> configs = current.getSnapshotConfigurations();
            out.writeVInt(configs.size());
            for (SnapshotLifecyclePolicyMetadata meta : configs.values()) {
                writePolicy(out, meta, encryptionService);
            }
            return out.bytes().array();
        } catch (IOException e) {
            throw new RuntimeException("failed to serialize SnapshotLifecycleMetadata for snapshot", e);
        }
    }

    /**
     * Deserializes bytes produced by {@link #toSnapshotBytes} and re-encrypts each policy's password
     * under the destination cluster's PEK. Returns a complete {@link SnapshotLifecycleMetadata} that
     * replaces the one restored from the snapshot's global state (which would have unusable source-PEK-encrypted
     * passwords).
     */
    @Override
    public SnapshotLifecycleMetadata fromSnapshotBytes(byte[] bytes, EncryptionService encryptionService) {
        try (StreamInput in = StreamInput.wrap(bytes)) {
            int version = in.readVInt();
            if (version != FORMAT_VERSION) {
                throw new IllegalArgumentException("unsupported SLM snapshot format version: " + version);
            }
            int count = in.readVInt();
            Map<String, SnapshotLifecyclePolicyMetadata> configs = new HashMap<>(count);
            for (int i = 0; i < count; i++) {
                SnapshotLifecyclePolicyMetadata meta = readPolicy(in, encryptionService);
                configs.put(meta.getId(), meta);
            }
            return new SnapshotLifecycleMetadata(configs, OperationMode.RUNNING, null);
        } catch (IOException e) {
            throw new RuntimeException("failed to deserialize SnapshotLifecycleMetadata from snapshot bytes", e);
        }
    }

    /**
     * Per-policy wire format (written by {@link #writePolicy}, read by {@link #readPolicy}):
     * <pre>
     *   String: id
     *   String: name
     *   String: schedule
     *   String: repository
     *   GenericMap: configuration (encryption_password stripped if present)
     *   Optional{@literal <}SnapshotRetentionConfiguration{@literal >}: retention
     *   Optional{@literal <}String{@literal >}: unhealthyIfNoSnapshotWithin (string representation)
     *   boolean: hasPassword; if true: byte[]: plaintextPassword
     *   GenericMap: headers
     *   vlong: version
     *   vlong: modifiedDate
     *   Optional{@literal <}SnapshotInvocationRecord{@literal >}: lastSuccess
     *   Optional{@literal <}SnapshotInvocationRecord{@literal >}: lastFailure
     *   vlong: invocationsSinceLastSuccess
     * </pre>
     */
    private static void writePolicy(BytesStreamOutput out, SnapshotLifecyclePolicyMetadata meta, EncryptionService encryptionService)
        throws IOException {
        SnapshotLifecyclePolicy policy = meta.getPolicy();
        out.writeString(policy.getId());
        out.writeString(policy.getName());
        out.writeString(policy.getSchedule());
        out.writeString(policy.getRepository());
        Map<String, Object> config = policy.getConfig();
        if (config != null && config.containsKey("encryption_password")) {
            Map<String, Object> stripped = new HashMap<>(config);
            stripped.remove("encryption_password");
            out.writeGenericMap(stripped);
        } else {
            out.writeGenericMap(config);
        }
        out.writeOptionalWriteable(policy.getRetentionPolicy());
        TimeValue unhealthy = policy.getUnhealthyIfNoSnapshotWithin();
        out.writeOptionalString(unhealthy != null ? unhealthy.getStringRep() : null);
        EncryptedData encPwd = policy.getEncryptedPassword();
        if (encPwd != null) {
            out.writeBoolean(true);
            byte[] plaintext = encryptionService.decrypt(encPwd);
            try {
                out.writeByteArray(plaintext);
            } finally {
                Arrays.fill(plaintext, (byte) 0);
            }
        } else {
            out.writeBoolean(false);
        }
        out.writeGenericValue(meta.getHeaders());
        out.writeVLong(meta.getVersion());
        out.writeVLong(meta.getModifiedDate());
        out.writeOptionalWriteable(meta.getLastSuccess());
        out.writeOptionalWriteable(meta.getLastFailure());
        out.writeVLong(meta.getInvocationsSinceLastSuccess());
    }

    @SuppressWarnings("unchecked")
    private static SnapshotLifecyclePolicyMetadata readPolicy(StreamInput in, EncryptionService encryptionService) throws IOException {
        String id = in.readString();
        String name = in.readString();
        String schedule = in.readString();
        String repository = in.readString();
        Map<String, Object> config = in.readGenericMap();
        SnapshotRetentionConfiguration retention = in.readOptionalWriteable(SnapshotRetentionConfiguration::new);
        String unhealthyStr = in.readOptionalString();
        TimeValue unhealthy = unhealthyStr != null ? TimeValue.parseTimeValue(unhealthyStr, "unhealthy_if_no_snapshot_within") : null;
        EncryptedData encPwd = null;
        if (in.readBoolean()) {
            byte[] plaintextPwd = in.readByteArray();
            try {
                encPwd = encryptionService.encrypt(plaintextPwd);
            } finally {
                Arrays.fill(plaintextPwd, (byte) 0);
            }
        }
        SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(id, name, schedule, repository, config, retention, unhealthy, encPwd);

        Map<String, String> headers = (Map<String, String>) in.readGenericValue();
        long version = in.readVLong();
        long modifiedDate = in.readVLong();
        SnapshotInvocationRecord lastSuccess = in.readOptionalWriteable(SnapshotInvocationRecord::new);
        SnapshotInvocationRecord lastFailure = in.readOptionalWriteable(SnapshotInvocationRecord::new);
        long invocationsSinceLastSuccess = in.readVLong();

        return SnapshotLifecyclePolicyMetadata.builder()
            .setPolicy(policy)
            .setHeaders(headers != null ? headers : Map.of())
            .setVersion(version)
            .setModifiedDate(modifiedDate)
            .setLastSuccess(lastSuccess)
            .setLastFailure(lastFailure)
            .setInvocationsSinceLastSuccess(invocationsSinceLastSuccess)
            .build();
    }
}
