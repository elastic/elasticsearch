/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;

import java.text.ParseException;

import static org.hamcrest.Matchers.containsString;

public class JwtAuthenticatorAccessTokenTypeTests extends JwtAuthenticatorTests {

    @Override
    protected JwtRealmSettings.TokenType getTokenType() {
        return JwtRealmSettings.TokenType.ACCESS_TOKEN;
    }

    public void testSubjectIsRequired() throws ParseException {
        final IllegalArgumentException e = doTestSubjectIsRequired(buildJwtAuthenticator());
        if (fallbackSub != null) {
            assertThat(e.getMessage(), containsString("missing required string claim [" + fallbackSub + " (fallback of sub)]"));
        }
    }

    public void testAccessTokenTypeMandatesAllowedSubjects() {
        allowedSubject = null;
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> buildJwtAuthenticator());

        assertThat(
            e.getMessage(),
            containsString("Invalid empty list for [" + RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_SUBJECTS) + "]")
        );
    }

    public void testInvalidIssuerIsCheckedBeforeAlgorithm() throws ParseException {
        doTestInvalidIssuerIsCheckedBeforeAlgorithm(buildJwtAuthenticator());
    }
}
