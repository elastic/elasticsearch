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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public final class DelegatePkiAuthenticationResponse {

    private final String accessToken;
    private final String type;
    private final TimeValue expiresIn;

    public DelegatePkiAuthenticationResponse(String accessToken, String type, TimeValue expiresIn) {
        this.accessToken = accessToken;
        this.type = type;
        this.expiresIn = expiresIn;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public String getType() {
        return type;
    }

    public TimeValue getExpiresIn() {
        return expiresIn;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final DelegatePkiAuthenticationResponse that = (DelegatePkiAuthenticationResponse) o;
        return Objects.equals(accessToken, that.accessToken) &&
            Objects.equals(type, that.type) &&
            Objects.equals(expiresIn, that.expiresIn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(accessToken, type, expiresIn);
    }

    private static final ConstructingObjectParser<DelegatePkiAuthenticationResponse, Void> PARSER = new ConstructingObjectParser<>(
            "delegate_pki_response", true,
            args -> new DelegatePkiAuthenticationResponse((String) args[0], (String) args[1], TimeValue.timeValueSeconds((Long) args[2])));

    static {
        PARSER.declareString(constructorArg(), new ParseField("access_token"));
        PARSER.declareString(constructorArg(), new ParseField("type"));
        PARSER.declareLong(constructorArg(), new ParseField("expires_in"));
    }

    public static DelegatePkiAuthenticationResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
