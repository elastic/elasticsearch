/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License v 1".
 */
package org.elasticsearch.plugin.security.cloudiam;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;

import java.util.Locale;

public class MockIamClient implements IamClient {
    private final String expectedSignature;
    private final String arnTemplate;
    private final String accountId;

    public MockIamClient(RealmConfig config) {
        this.expectedSignature = config.getSetting(CloudIamRealmSettings.MOCK_SIGNATURE, () -> "mock");
        this.arnTemplate = config.getSetting(CloudIamRealmSettings.MOCK_ARN_TEMPLATE, () -> "acs:ram::000000000000:user/%s");
        this.accountId = config.getSetting(CloudIamRealmSettings.MOCK_ACCOUNT_ID, () -> "000000000000");
    }

    @Override
    public void verify(CloudIamToken token, ActionListener<IamPrincipal> listener) {
        if (Strings.hasText(expectedSignature) && expectedSignature.equals(token.signature()) == false) {
            listener.onFailure(new IllegalArgumentException("mock signature mismatch"));
            return;
        }
        String arn = String.format(Locale.ROOT, arnTemplate, token.accessKeyId());
        listener.onResponse(new IamPrincipal(arn, accountId, null, IamPrincipal.principalTypeFromArn(arn)));
    }
}
