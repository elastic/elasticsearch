/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.ccm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.SecureString;

import java.util.Objects;

public class ValidationAuthenticationFactory implements AuthenticationFactory {

    private final SecureString apiKey;

    public ValidationAuthenticationFactory(SecureString apiKey) {
        this.apiKey = Objects.requireNonNull(apiKey);
    }

    public void getAuthenticationApplier(ActionListener<CCMAuthenticationApplierFactory.AuthApplier> listener) {
        listener.onResponse(new CCMAuthenticationApplierFactory.AuthenticationHeaderApplier(apiKey));
    }
}
