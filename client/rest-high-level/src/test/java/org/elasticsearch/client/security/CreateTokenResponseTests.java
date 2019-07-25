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

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class CreateTokenResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        final String accessToken = randomAlphaOfLengthBetween(12, 24);
        final TimeValue expiresIn = TimeValue.timeValueSeconds(randomIntBetween(30, 10_000));
        final String refreshToken = randomBoolean() ? null : randomAlphaOfLengthBetween(12, 24);
        final String scope = randomBoolean() ? null : randomAlphaOfLength(4);
        final String type = randomAlphaOfLength(6);
        final String kerberosAuthenticationResponseToken = randomBoolean() ? null : randomAlphaOfLength(7);

        final XContentType xContentType = randomFrom(XContentType.values());
        final XContentBuilder builder = XContentFactory.contentBuilder(xContentType);
        builder.startObject()
            .field("access_token", accessToken)
            .field("type", type)
            .field("expires_in", expiresIn.seconds());
        if (refreshToken != null || randomBoolean()) {
            builder.field("refresh_token", refreshToken);
        }
        if (scope != null || randomBoolean()) {
            builder.field("scope", scope);
        }
        if (kerberosAuthenticationResponseToken != null) {
            builder.field("kerberos_authentication_response_token", kerberosAuthenticationResponseToken);
        }
        builder.endObject();
        BytesReference xContent = BytesReference.bytes(builder);

        final CreateTokenResponse response = CreateTokenResponse.fromXContent(createParser(xContentType.xContent(), xContent));
        assertThat(response.getAccessToken(), equalTo(accessToken));
        assertThat(response.getRefreshToken(), equalTo(refreshToken));
        assertThat(response.getScope(), equalTo(scope));
        assertThat(response.getType(), equalTo(type));
        assertThat(response.getExpiresIn(), equalTo(expiresIn));
        assertThat(response.getKerberosAuthenticationResponseToken(), equalTo(kerberosAuthenticationResponseToken));
    }
}
