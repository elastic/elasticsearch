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

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class InvalidateTokenRequestTests extends ESTestCase {

    public void testInvalidateAccessToken() {
        String token = "Tf01rrAymdUjxMY4VlG3gV3gsFFUWxVVPrztX+4uhe0=";
        final InvalidateTokenRequest request = InvalidateTokenRequest.accessToken(token);
        assertThat(request.getAccessToken(), equalTo(token));
        assertThat(request.getRefreshToken(), nullValue());
        assertThat(Strings.toString(request), equalTo("{" +
            "\"token\":\"Tf01rrAymdUjxMY4VlG3gV3gsFFUWxVVPrztX+4uhe0=\"" +
            "}"
        ));
    }

    public void testInvalidateRefreshToken() {
        String token = "4rE0YPT/oHODS83TbTtYmuh8";
        final InvalidateTokenRequest request = InvalidateTokenRequest.refreshToken(token);
        assertThat(request.getAccessToken(), nullValue());
        assertThat(request.getRefreshToken(), equalTo(token));
        assertThat(Strings.toString(request), equalTo("{" +
            "\"refresh_token\":\"4rE0YPT/oHODS83TbTtYmuh8\"" +
            "}"
        ));
    }

    public void testEqualsAndHashCode() {
        final String token = randomAlphaOfLength(8);
        final boolean accessToken = randomBoolean();
        final InvalidateTokenRequest request = accessToken ? InvalidateTokenRequest.accessToken(token)
            : InvalidateTokenRequest.refreshToken(token);
        final EqualsHashCodeTestUtils.MutateFunction<InvalidateTokenRequest> mutate = r -> {
            if (randomBoolean()) {
                return accessToken ? InvalidateTokenRequest.refreshToken(token) : InvalidateTokenRequest.accessToken(token);
            } else {
                return accessToken ? InvalidateTokenRequest.accessToken(randomAlphaOfLength(10))
                    : InvalidateTokenRequest.refreshToken(randomAlphaOfLength(10));
            }
        };
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(request,
            r -> new InvalidateTokenRequest(r.getAccessToken(), r.getRefreshToken()), mutate);
    }
}
