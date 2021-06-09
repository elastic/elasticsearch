/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.saml.sp;

import org.elasticsearch.core.TimeValue;

import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomLong;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.opensaml.saml.saml2.core.NameIDType.TRANSIENT;

public class SamlServiceProviderTestUtils {

    private SamlServiceProviderTestUtils(){} //utility class

    public static SamlServiceProviderDocument randomDocument() {
        return randomDocument(randomIntBetween(1, 999_999));
    }

    public static SamlServiceProviderDocument randomDocument(int index) {
        final SamlServiceProviderDocument document = new SamlServiceProviderDocument();
        document.setName(randomAlphaOfLengthBetween(5, 12));
        document.setEntityId(randomUri() + index);
        document.setAcs(randomUri("https") + index + "/saml/acs");

        document.setEnabled(randomBoolean());
        document.setCreatedMillis(System.currentTimeMillis() - TimeValue.timeValueDays(randomIntBetween(2, 90)).millis());
        document.setLastModifiedMillis(System.currentTimeMillis() - TimeValue.timeValueHours(randomIntBetween(1, 36)).millis());

        if (randomBoolean()) {
            document.setNameIdFormat(TRANSIENT);
        }
        if (randomBoolean()) {
            document.setAuthenticationExpiryMillis(TimeValue.timeValueMinutes(randomIntBetween(1, 15)).millis());
        }

        document.privileges.setResource("app:" + randomAlphaOfLengthBetween(3, 6) + ":" + Math.abs(randomLong()));
        final int roleCount = randomIntBetween(0, 4);
        final Set<String> roles = new HashSet<>();
        for (int i = 0; i < roleCount; i++) {
            roles.add(randomAlphaOfLengthBetween(3, 6) + ":(" + randomAlphaOfLengthBetween(3, 6) + ")");
        }
        document.privileges.setRolePatterns(roles);

        document.attributeNames.setPrincipal(randomUri());
        if (randomBoolean()) {
            document.attributeNames.setName(randomUri());
        }
        if (randomBoolean()) {
            document.attributeNames.setEmail(randomUri());
        }
        if (roles.isEmpty() == false) {
            document.attributeNames.setRoles(randomUri());
        }

        assertThat(document.validate(), nullValue());
        return document;
    }

    private static String randomUri() {
        return randomUri(randomFrom("urn", "http", "https"));
    }

    private static String randomUri(String scheme) {
        return scheme + "://" + randomAlphaOfLengthBetween(2, 6) + "."
            + randomAlphaOfLengthBetween(4, 8) + "." + randomAlphaOfLengthBetween(2, 4) + "/";
    }
}
