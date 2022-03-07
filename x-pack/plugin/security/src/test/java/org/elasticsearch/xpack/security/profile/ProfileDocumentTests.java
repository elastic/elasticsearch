/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.profile;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTests;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmDomain;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.hasSize;

public class ProfileDocumentTests extends ESTestCase {

    // Same uid should be used for the same username and domain definition (if domain is configured) or realm
    public void testSameUid() {
        final String username = randomAlphaOfLengthBetween(5, 12);

        final RealmDomain realmDomain = randomFrom(AuthenticationTests.randomDomain(true), null);
        final Authentication.RealmRef realmRefWithoutDomain = AuthenticationTests.randomRealmRef(false);

        final Set<String> allUids = new HashSet<>();
        IntStream.range(0, 10).forEach(i -> {
            final Authentication.RealmRef realmRef;
            if (realmDomain == null) {
                realmRef = realmRefWithoutDomain;
            } else {
                final RealmConfig.RealmIdentifier realmIdentifier = randomFrom(realmDomain.realms());
                realmRef = new Authentication.RealmRef(
                    realmIdentifier.getName(),
                    realmIdentifier.getType(),
                    randomAlphaOfLengthBetween(3, 8)
                );
            }
            final ProfileDocument profileDocument = ProfileDocument.fromSubject(new Subject(new User(username), realmRef));
            allUids.add(profileDocument.uid());
        });
        assertThat(allUids, hasSize(1));
    }
}
