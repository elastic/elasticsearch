/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.ElasticUser;
import org.elasticsearch.xpack.core.security.user.KibanaSystemUser;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class GetUsersResponseTests extends ESTestCase {

    public void testToXContent() throws IOException {

        final Settings settings = Settings.builder()
            .put(AnonymousUser.USERNAME_SETTING.getKey(), "_" + randomAlphaOfLengthBetween(5, 18))
            .put(AnonymousUser.ROLES_SETTING.getKey(), "superuser")
            .build();

        final User reservedUser = randomFrom(new ElasticUser(true), new KibanaSystemUser(true));
        final User anonymousUser = new AnonymousUser(settings);
        final User nativeUser = AuthenticationTestHelper.randomUser();

        final Map<String, String> profileUidLookup;
        if (randomBoolean()) {
            profileUidLookup = new HashMap<>();
            if (randomBoolean()) {
                profileUidLookup.put(reservedUser.principal(), "u_profile_" + reservedUser.principal());
            }
            if (randomBoolean()) {
                profileUidLookup.put(anonymousUser.principal(), "u_profile_" + anonymousUser.principal());
            }
            if (randomBoolean()) {
                profileUidLookup.put(nativeUser.principal(), "u_profile_" + nativeUser.principal());
            }
        } else {
            profileUidLookup = null;
        }

        final var response = new GetUsersResponse(List.of(reservedUser, anonymousUser, nativeUser), profileUidLookup);
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);

        assertThat(
            Strings.toString(builder),
            equalTo(
                XContentHelper.stripWhitespace(
                    Strings.format(
                        """
                            {
                              "%s": {
                                "username": "%s",
                                "roles": [
                                  %s
                                ],
                                "full_name": null,
                                "email": null,
                                "metadata": {
                                  "_reserved": true
                                },
                                "enabled": true%s
                              },
                              "%s": {
                                "username": "%s",
                                "roles": [
                                  "superuser"
                                ],
                                "full_name": null,
                                "email": null,
                                "metadata": {
                                  "_reserved": true
                                },
                                "enabled": true%s
                              },
                              "%s": {
                                "username": "%s",
                                "roles": [
                                  %s
                                ],
                                "full_name": null,
                                "email": null,
                                "metadata": {},
                                "enabled": true%s
                              }
                            }""",
                        reservedUser.principal(),
                        reservedUser.principal(),
                        getRolesOutput(reservedUser),
                        getProfileUidOutput(reservedUser, profileUidLookup),
                        anonymousUser.principal(),
                        anonymousUser.principal(),
                        getProfileUidOutput(anonymousUser, profileUidLookup),
                        nativeUser.principal(),
                        nativeUser.principal(),
                        getRolesOutput(nativeUser),
                        getProfileUidOutput(nativeUser, profileUidLookup)
                    )
                )
            )
        );
    }

    private String getRolesOutput(User user) {
        return Arrays.stream(user.roles()).map(r -> "\"" + r + "\"").collect(Collectors.joining(","));
    }

    private String getProfileUidOutput(User user, Map<String, String> profileUidLookup) {
        if (profileUidLookup == null) {
            return "";
        } else {
            final String uid = profileUidLookup.get(user.principal());
            if (uid == null) {
                return "";
            } else {
                return ", \"profile_uid\": \"" + uid + "\"";
            }
        }
    }
}
