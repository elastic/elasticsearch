/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.settings.secure;

import org.elasticsearch.action.admin.cluster.settings.UpdateSecureSettingsAction;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandlerProvider;

import java.util.Collection;
import java.util.List;

public final class SecureSettingsEncryptedDataHandlerProvider implements EncryptedDataHandlerProvider {

    @Override
    public Collection<EncryptedDataHandler<?>> getHandlers() {
        if (UpdateSecureSettingsAction.SECURE_SETTINGS_API_FEATURE_FLAG.isEnabled() == false) {
            return List.of();
        }
        return List.of(new SecureSettingsEncryptedDataHandler());
    }
}
