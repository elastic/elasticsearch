/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.core.encryption.EncryptedData;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * Re-keys SLM policy encryption passwords. Each {@link SnapshotLifecyclePolicy} may carry an {@link EncryptedData}
 * password field; this handler knows where those fields live and applies the caller-supplied re-keying function to
 * them (key rotation, snapshot-password re-wrapping, and restore re-wrapping all flow through the same traversal).
 */
public final class SlmEncryptedDataHandler implements EncryptedDataHandler<SnapshotLifecycleMetadata> {

    private static final Logger logger = LogManager.getLogger(SlmEncryptedDataHandler.class);

    @Override
    public String customName() {
        return SnapshotLifecycleMetadata.TYPE;
    }

    @Override
    public SnapshotLifecycleMetadata reEncrypt(SnapshotLifecycleMetadata current, UnaryOperator<EncryptedData> rewrap) {
        Map<String, SnapshotLifecyclePolicyMetadata> rebuilt = null;
        for (Map.Entry<String, SnapshotLifecyclePolicyMetadata> entry : current.getSnapshotConfigurations().entrySet()) {
            SnapshotLifecyclePolicyMetadata policyMeta = entry.getValue();
            EncryptedData existing = policyMeta.getPolicy().getEncryptedPassword();
            if (existing == null) {
                continue;
            }
            EncryptedData rewrapped = rewrap.apply(existing);
            if (rewrapped != existing) {
                if (rebuilt == null) {
                    rebuilt = new HashMap<>(current.getSnapshotConfigurations());
                }
                SnapshotLifecyclePolicy newPolicy = policyMeta.getPolicy().withEncryptedPassword(rewrapped);
                rebuilt.put(entry.getKey(), SnapshotLifecyclePolicyMetadata.builder(policyMeta).setPolicy(newPolicy).build());
            }
        }
        return rebuilt != null ? current.withSnapshotConfigurations(rebuilt) : current;
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
        SnapshotLifecycleMetadata result = reEncrypt(current, existing -> null);
        if (result != current) {
            List<String> clearedPolicies = current.getSnapshotConfigurations()
                .values()
                .stream()
                .filter(meta -> meta.getPolicy().getEncryptedPassword() != null)
                .map(SnapshotLifecyclePolicyMetadata::getId)
                .sorted()
                .toList();
            logger.warn(
                "destructive encryption reset permanently destroyed the snapshot encryption passwords of SLM policies {}; "
                    + "the policies remain active but their snapshots will exclude any encrypted values and will not be "
                    + "password-protected until each policy is updated with a new encrypted_data configuration",
                clearedPolicies
            );
        }
        return result;
    }
}
