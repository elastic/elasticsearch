/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.token;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.user.User;

public class CreateTokenResponseTests extends ESTestCase {

    public void testSerialization() throws Exception {
        CreateTokenResponse response = new CreateTokenResponse(
            randomAlphaOfLengthBetween(1, 10),
            TimeValue.timeValueMinutes(20L),
            randomBoolean() ? null : "FULL",
            randomAlphaOfLengthBetween(1, 10),
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 10),
            AuthenticationTestHelper.builder()
                .user(new User("bar", "not_superuser"))
                .realmRef(new Authentication.RealmRef("test", "test", "node"))
                .runAs()
                .user(new User("joe", "custom_superuser"))
                .realmRef(new Authentication.RealmRef("test", "test", "node"))
                .build()
        );
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            response.writeTo(output);
            try (StreamInput input = output.bytes().streamInput()) {
                CreateTokenResponse serialized = new CreateTokenResponse(input);
                assertEquals(response, serialized);
            }
        }

        response = new CreateTokenResponse(
            randomAlphaOfLengthBetween(1, 10),
            TimeValue.timeValueMinutes(20L),
            randomBoolean() ? null : "FULL",
            null,
            null,
            AuthenticationTestHelper.builder()
                .user(new User("bar", "not_superuser"))
                .realmRef(new Authentication.RealmRef("test", "test", "node"))
                .runAs()
                .user(new User("joe", "custom_superuser"))
                .realmRef(new Authentication.RealmRef("test", "test", "node"))
                .build()
        );
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            response.writeTo(output);
            try (StreamInput input = output.bytes().streamInput()) {
                CreateTokenResponse serialized = new CreateTokenResponse(input);
                assertEquals(response, serialized);
            }
        }
    }
}
