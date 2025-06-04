/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.profile;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.hamcrest.Matchers.equalTo;

public class GetProfilesResponseTests extends ESTestCase {

    public void testToXContent() throws IOException {
        final boolean hasErrors = randomBoolean();

        final Map<String, Exception> errors;
        if (hasErrors) {
            // Force ordered key set for deterministic comparison with raw JSON string below
            errors = new TreeMap<>();
            errors.put("u_user_foo_bar_1", new IllegalArgumentException("msg1"));
            errors.put("u_user_foo_bar_2", new ResourceNotFoundException("not found"));
            errors.put("u_user_foo_bar_3", new ElasticsearchException("error1", new IllegalArgumentException("msg2")));
        } else {
            errors = Map.of();
        }

        final boolean hasProfiles = randomBoolean() || false == hasErrors;
        final List<Profile> profiles;
        if (hasProfiles) {
            profiles = List.of(
                new Profile(
                    "u_profile_user_0",
                    true,
                    0L,
                    new Profile.ProfileUser("profile_user", List.of("user"), "realm_1", null, null, null),
                    Map.of("label", "value"),
                    Map.of("data", "value2"),
                    new Profile.VersionControl(1, 1)
                ),
                new Profile(
                    "u_profile_admin_0",
                    false,
                    1L,
                    new Profile.ProfileUser("profile_admin", List.of("admin"), "realm_2", "domain_2", "admin@example.org", "profile admin"),
                    Map.of(),
                    Map.of(),
                    new Profile.VersionControl(2, 2)
                )
            );
        } else {
            profiles = List.of();
        }

        final var response = new GetProfilesResponse(profiles, errors);
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);

        final StringBuilder sb = new StringBuilder("{ \"profiles\": ");
        if (hasProfiles) {
            sb.append("""
                [
                  {
                    "uid": "u_profile_user_0",
                    "enabled": true,
                    "last_synchronized": 0,
                    "user": {
                      "username": "profile_user",
                      "roles": [
                        "user"
                      ],
                      "realm_name": "realm_1"
                    },
                    "labels": {
                      "label": "value"
                    },
                    "data": {
                      "data": "value2"
                    },
                    "_doc": {
                      "_primary_term": 1,
                      "_seq_no": 1
                    }
                  },
                  {
                    "uid": "u_profile_admin_0",
                    "enabled": false,
                    "last_synchronized": 1,
                    "user": {
                      "username": "profile_admin",
                      "roles": [
                        "admin"
                      ],
                      "realm_name": "realm_2",
                      "realm_domain": "domain_2",
                      "email": "admin@example.org",
                      "full_name": "profile admin"
                    },
                    "labels": {},
                    "data": {},
                    "_doc": {
                      "_primary_term": 2,
                      "_seq_no": 2
                    }
                  }
                ]""");
        } else {
            sb.append("[]");
        }

        if (hasErrors) {
            sb.append(", \"errors\": ");
            sb.append("""
                {
                  "count": 3,
                  "details": {
                    "u_user_foo_bar_1": {
                      "type": "illegal_argument_exception",
                      "reason": "msg1"
                    },
                    "u_user_foo_bar_2": {
                      "type": "resource_not_found_exception",
                      "reason": "not found"
                    },
                    "u_user_foo_bar_3": {
                      "type": "exception",
                      "reason": "error1",
                      "caused_by": {
                        "type": "illegal_argument_exception",
                        "reason": "msg2"
                      }
                    }
                  }
                }
                """);
        }
        sb.append("}");

        assertThat(Strings.toString(builder), equalTo(XContentHelper.stripWhitespace(sb.toString())));
    }
}
