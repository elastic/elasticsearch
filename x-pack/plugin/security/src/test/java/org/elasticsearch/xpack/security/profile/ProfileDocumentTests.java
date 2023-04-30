/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.profile;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class ProfileDocumentTests extends ESTestCase {

    // Same uid should be used for the same username irrespective of the user's realm and domain
    public void testSameUid() {
        final String username = randomAlphaOfLengthBetween(5, 12);

        final Set<String> allUids = new HashSet<>();
        IntStream.range(0, 10).forEach(i -> {
            final Authentication.RealmRef realmRef = AuthenticationTestHelper.randomRealmRef(randomBoolean());
            final User user = new User(
                username,
                randomArray(1, 3, String[]::new, () -> randomAlphaOfLengthBetween(3, 8)),
                randomAlphaOfLengthBetween(3, 18),
                randomAlphaOfLengthBetween(8, 18),
                randomMap(
                    0,
                    5,
                    () -> new Tuple<>(randomAlphaOfLengthBetween(3, 8), randomFrom(randomAlphaOfLengthBetween(3, 8), randomInt()))
                ),
                randomBoolean()
            );
            final ProfileDocument profileDocument = ProfileDocument.fromSubject(new Subject(user, realmRef));
            allUids.add(profileDocument.uid());
        });
        assertThat(allUids, hasSize(1));
    }

    public void testDifferentUids() {
        final String username0 = randomAlphaOfLengthBetween(5, 12);
        final String username1 = randomValueOtherThan(username0, () -> randomAlphaOfLengthBetween(5, 12));

        final Authentication.RealmRef realmRef0 = AuthenticationTestHelper.randomRealmRef(randomBoolean());
        final Authentication.RealmRef realmRef1 = randomBoolean() ? realmRef0 : AuthenticationTestHelper.randomRealmRef(randomBoolean());

        assertThat(
            ProfileDocument.fromSubject(new Subject(new User(username0), realmRef0)),
            not(equalTo(ProfileDocument.fromSubject(new Subject(new User(username1), realmRef1))))
        );
    }
}
