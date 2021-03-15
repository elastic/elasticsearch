/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xpack.security.authc.service.ServiceAccount.ServiceAccountId;
import org.elasticsearch.xpack.security.authc.support.SecurityTokenType;

import java.io.IOException;
import java.util.Base64;

/**
 * A decoded credential that may be used to authenticate a {@link ServiceAccount}.
 * It consists of:
 * <ol>
 *   <li>A {@link #getAccountId() service account id}</li>
 *   <li>The {@link #getTokenName() name of the token} to be used</li>
 *   <li>The {@link #getSecret() secreet credential} for that token</li>
 * </ol>
 */
public class ServiceAccountToken {
    private final ServiceAccountId accountId;
    private final String tokenName;
    private final SecureString secret;

    public ServiceAccountToken(ServiceAccountId accountId, String tokenName, SecureString secret) {
        this.accountId = accountId;
        this.tokenName = tokenName;
        this.secret = secret;
    }

    public ServiceAccountId getAccountId() {
        return accountId;
    }

    public String getTokenName() {
        return tokenName;
    }

    public String getQualifiedName() {
        return getAccountId().asPrincipal() + '/' + tokenName;
    }

    public SecureString getSecret() {
        return secret;
    }

    public SecureString asBearerString() throws IOException {
        try(
            BytesStreamOutput out = new BytesStreamOutput()) {
            Version.writeVersion(Version.CURRENT, out);
            SecurityTokenType.SERVICE_ACCOUNT.write(out);
            accountId.write(out);
            out.writeString(tokenName);
            out.writeSecureString(secret);
            out.flush();

            final String base64 = Base64.getEncoder().withoutPadding().encodeToString(out.bytes().toBytesRef().bytes);
            return new SecureString(base64.toCharArray());
        }
    }
}
