/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.security.user.User;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class AuthenticateResponseTests extends ESTestCase {
    
    public void testFromXContent() throws IOException {
        xContentTester(
                this::createParser,
                this::createTestInstance,
                this::toXContent,
                AuthenticateResponse::fromXContent)
                .supportsUnknownFields(false)
                .randomFieldsExcludeFilter(field ->
                        field.endsWith("status.current_position"))
                .test();
    }

    protected AuthenticateResponse createTestInstance() {
        final String username = randomAlphaOfLengthBetween(1, 4);
        final List<String> roles = Arrays.asList(generateRandomStringArray(4, 4, false, true));
        final Map<String, Object> metadata;
        metadata = new HashMap<>();
        if (randomBoolean()) {
            metadata.put("string", null);
        } else {
            metadata.put("string", randomAlphaOfLengthBetween(0, 4));
        }
        if (randomBoolean()) {
            metadata.put("string_list", null);
        } else {
            metadata.put("string_list", Arrays.asList(generateRandomStringArray(4, 4, false, true)));
        }
        final String fullName = randomFrom(random(), null, randomAlphaOfLengthBetween(0, 4));
        final String email = randomFrom(random(), null, randomAlphaOfLengthBetween(0, 4));
        final boolean enabled = randomBoolean();
        return new AuthenticateResponse(new User(username, roles, metadata, fullName, email), enabled);
    }
    
    private void toXContent(AuthenticateResponse response, XContentBuilder builder) throws IOException {
        final User user = response.getUser();
        final boolean enabled = response.enabled();
        builder.startObject();
        builder.field(AuthenticateResponse.USERNAME.getPreferredName(), user.username());
        builder.field(AuthenticateResponse.ROLES.getPreferredName(), user.roles());
        builder.field(AuthenticateResponse.METADATA.getPreferredName(), user.metadata());
        if (user.fullName() != null) {
            builder.field(AuthenticateResponse.FULL_NAME.getPreferredName(), user.fullName());
        }
        if (user.email() != null) {
            builder.field(AuthenticateResponse.EMAIL.getPreferredName(), user.email());
        }
        builder.field(AuthenticateResponse.ENABLED.getPreferredName(), enabled);
        builder.endObject();
    }
    
}
