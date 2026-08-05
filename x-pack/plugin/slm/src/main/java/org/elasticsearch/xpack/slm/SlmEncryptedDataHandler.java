/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm;

import org.elasticsearch.xpack.core.encryption.EncryptedData;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * Re-keys SLM policy encryption passwords. Each {@link SnapshotLifecyclePolicy} may carry an {@link EncryptedData}
 * password field; this handler knows where those fields live and applies the caller-supplied re-keying function to
 * them (key rotation, snapshot-password re-wrapping, and restore re-wrapping all flow through the same traversal).
 */
public final class SlmEncryptedDataHandler implements EncryptedDataHandler<SnapshotLifecycleMetadata> {

    @Override
    public String customName() {
        return SnapshotLifecycleMetadata.TYPE;
    }

    @Override
    @SuppressWarnings("deprecation")
    public SnapshotLifecycleMetadata reEncrypt(SnapshotLifecycleMetadata current, UnaryOperator<EncryptedData> rewrap) {
        boolean changed = false;
        Map<String, SnapshotLifecyclePolicyMetadata> rebuilt = new HashMap<>(current.getSnapshotConfigurations());
        for (Map.Entry<String, SnapshotLifecyclePolicyMetadata> entry : current.getSnapshotConfigurations().entrySet()) {
            SnapshotLifecyclePolicyMetadata policyMeta = entry.getValue();
            EncryptedData existing = policyMeta.getPolicy().getEncryptedPassword();
            if (existing == null) {
                continue;
            }
            EncryptedData rewrapped = rewrap.apply(existing);
            if (rewrapped != existing) {
                SnapshotLifecyclePolicy newPolicy = policyMeta.getPolicy().withEncryptedPassword(rewrapped);
                rebuilt.put(entry.getKey(), SnapshotLifecyclePolicyMetadata.builder(policyMeta).setPolicy(newPolicy).build());
                changed = true;
            }
        }
        return changed ? new SnapshotLifecycleMetadata(rebuilt, current.getOperationMode(), current.getStats()) : current;
    }

    /**
     * On destructive reset, wipe the encryption passwords from all SLM policies while preserving the rest of each
     * policy's configuration. The policies remain functional but will no longer use a password when creating snapshots.
     */
    @Override
    public SnapshotLifecycleMetadata onDestructiveReset(SnapshotLifecycleMetadata current) {
        if (current == null) {
            return null;
        }
        return reEncrypt(current, existing -> null);
    }
}
