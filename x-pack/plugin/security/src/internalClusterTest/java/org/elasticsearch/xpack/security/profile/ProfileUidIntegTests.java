/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.profile;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.security.action.profile.Profile;
import org.junit.BeforeClass;

import java.util.Base64;

import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class ProfileUidIntegTests extends AbstractProfileIntegTestCase {

    private static boolean HAS_DOMAIN1;
    private static boolean HAS_DOMAIN2;
    private static boolean DOMAIN1_LITERAL_USERNAME;
    private static boolean DOMAIN2_LITERAL_USERNAME;
    private static String SUFFIX_BASE;

    @BeforeClass
    public static void initDomainConfig() {
        HAS_DOMAIN1 = randomBoolean();
        HAS_DOMAIN2 = randomBoolean();
        DOMAIN1_LITERAL_USERNAME = HAS_DOMAIN1 && randomBoolean();
        DOMAIN2_LITERAL_USERNAME = HAS_DOMAIN2 && randomBoolean();
        SUFFIX_BASE = randomAlphaOfLengthBetween(1, 9);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        final Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        if (HAS_DOMAIN1) {
            builder.put("xpack.security.authc.domains.domain1.realms", "file");
        }
        if (HAS_DOMAIN2) {
            builder.put("xpack.security.authc.domains.domain2.realms", "index");
        }
        if (DOMAIN1_LITERAL_USERNAME) {
            builder.put("xpack.security.authc.domains.domain1.uid_generation.literal_username", true);
            builder.put("xpack.security.authc.domains.domain1.uid_generation.suffix", SUFFIX_BASE + "1");
        }
        if (DOMAIN2_LITERAL_USERNAME) {
            builder.put("xpack.security.authc.domains.domain2.uid_generation.literal_username", true);
            builder.put("xpack.security.authc.domains.domain2.uid_generation.suffix", SUFFIX_BASE + "2");
        }
        return builder.build();
    }

    public void testActivateProfileUids() {
        final String hashedUsername = Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(MessageDigests.digest(new BytesArray(RAC_USER_NAME), MessageDigests.sha256()));

        // Activate 1st time with the file realm user
        final Profile profile1 = doActivateProfile(RAC_USER_NAME, TEST_PASSWORD_SECURE_STRING);
        assertThat(profile1.user().username(), equalTo(RAC_USER_NAME));
        assertThat(profile1.user().realmName(), equalTo("file"));
        if (HAS_DOMAIN1) {
            assertThat(profile1.user().domainName(), equalTo("domain1"));
        } else {
            assertThat(profile1.user().domainName(), nullValue());
        }

        if (DOMAIN1_LITERAL_USERNAME) {
            assertThat(profile1.uid(), equalTo("u_" + RAC_USER_NAME + "_" + SUFFIX_BASE + "1"));
        } else {
            assertThat(profile1.uid(), equalTo("u_" + hashedUsername + "_0"));
        }

        // Activate 2nd time with the native realm user
        final Profile profile2 = doActivateProfile(RAC_USER_NAME, NATIVE_RAC_USER_PASSWORD);
        assertThat(profile2.user().username(), equalTo(RAC_USER_NAME));
        assertThat(profile2.user().realmName(), equalTo("index"));
        if (HAS_DOMAIN2) {
            assertThat(profile2.user().domainName(), equalTo("domain2"));
        } else {
            assertThat(profile2.user().domainName(), nullValue());
        }

        if (DOMAIN2_LITERAL_USERNAME) {
            assertThat(profile2.uid(), equalTo("u_" + RAC_USER_NAME + "_" + SUFFIX_BASE + "2"));
        } else {
            if (DOMAIN1_LITERAL_USERNAME) {
                assertThat(profile2.uid(), equalTo("u_" + hashedUsername + "_0"));
            } else {
                // If both profiles use autonumber as suffix, the 2nd one should get autoincrement from 0 to 1
                assertThat(profile2.uid(), equalTo("u_" + hashedUsername + "_1"));
            }
        }

        // The two uids should never be the same since they are from different domains
        assertThat(profile1.uid(), not(equalTo(profile2.uid())));
    }
}
