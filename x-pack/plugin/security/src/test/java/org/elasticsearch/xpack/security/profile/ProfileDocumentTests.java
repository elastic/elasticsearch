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
import java.util.stream.Collectors;
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
            Authentication.RealmRef realmRef;
            if (realmDomain == null) {
                realmRef = realmRefWithoutDomain;
            } else {
                final RealmConfig.RealmIdentifier realmIdentifier = randomFrom(realmDomain.realms());
                realmRef = new Authentication.RealmRef(
                    realmIdentifier.getName(),
                    realmIdentifier.getType(),
                    randomAlphaOfLengthBetween(3, 8),
                    realmDomain
                );
            }
            realmRef = maybeMutateRealmRef(realmRef);
            final ProfileDocument profileDocument = ProfileDocument.fromSubject(new Subject(new User(username), realmRef));
            allUids.add(profileDocument.uid());
        });
        assertThat(allUids, hasSize(1));
    }

    private Authentication.RealmRef maybeMutateRealmRef(Authentication.RealmRef original) {
        final String realmName;
        if (Authentication.isFileOrNativeRealm(original.getType())) {
            realmName = randomAlphaOfLengthBetween(3, 8);
        } else {
            realmName = original.getName();
        }
        String nodeName = randomBoolean() ? randomAlphaOfLengthBetween(3, 8) : original.getNodeName();

        final RealmDomain realmDomain;
        if (original.getDomain() == null) {
            realmDomain = null;
        } else {
            final Set<RealmConfig.RealmIdentifier> realms;
            if (false == realmName.equals(original.getName())) {
                realms = original.getDomain().realms().stream().map(realmIdentifier -> {
                    if (realmIdentifier.getName().equals(original.getName())) {
                        return new RealmConfig.RealmIdentifier(original.getType(), realmName);
                    } else {
                        return realmIdentifier;
                    }
                }).collect(Collectors.toSet());
            } else {
                realms = original.getDomain().realms();
            }

            final String domainName = randomBoolean() ? randomAlphaOfLengthBetween(3, 8) : original.getDomain().name();
            realmDomain = new RealmDomain(domainName, realms);
        }
        return new Authentication.RealmRef(realmName, original.getType(), nodeName, realmDomain);
    }
}
