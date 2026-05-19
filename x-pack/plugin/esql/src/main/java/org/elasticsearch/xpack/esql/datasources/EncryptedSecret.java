/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.cluster.metadata.DataSourceSetting;

/**
 * Marks an encrypted secret as it travels in the transient connector-config {@code Map<String, Object>}
 * from the coordinator to the data-node decrypt seam.
 *
 * <p>This type exists so the decrypt seam never has to infer "is this encrypted" from a value's
 * runtime class. A bare {@code byte[]} in the config map would be ambiguous against a legitimate
 * binary plaintext value; an {@code EncryptedSecret} is unambiguous because it is constructed in
 * exactly one place (the settings projection for an encrypted setting) and nothing else ever
 * produces one. Its presence — not the shape of its contents — is the discriminator.
 *
 * <p>The map is live in-JVM only and never serialized, so a dedicated type is the right carrier here;
 * the persisted-record discriminator is {@code DataSourceSetting.EncryptionFormat} instead.
 */
public record EncryptedSecret(byte[] blob) {

    @Override
    public String toString() {
        return "EncryptedSecret[" + DataSourceSetting.MASK_SENTINEL + "]";
    }
}
