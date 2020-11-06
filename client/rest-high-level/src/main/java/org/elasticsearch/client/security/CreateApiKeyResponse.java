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

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Response for create API key
 */
public final class CreateApiKeyResponse {

    private final String name;
    private final String id;
    private final SecureString key;
    private final Instant expiration;

    public CreateApiKeyResponse(String name, String id, SecureString key, Instant expiration) {
        this.name = name;
        this.id = id;
        this.key = key;
        // As we do not yet support the nanosecond precision when we serialize to JSON,
        // here creating the 'Instant' of milliseconds precision.
        // This Instant can then be used for date comparison.
        this.expiration = (expiration != null) ? Instant.ofEpochMilli(expiration.toEpochMilli()): null;
    }

    public String getName() {
        return name;
    }

    public String getId() {
        return id;
    }

    public SecureString getKey() {
        return key;
    }

    @Nullable
    public Instant getExpiration() {
        return expiration;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, key, expiration);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final CreateApiKeyResponse other = (CreateApiKeyResponse) obj;
        return Objects.equals(id, other.id)
                && Objects.equals(key, other.key)
                && Objects.equals(name, other.name)
                && Objects.equals(expiration, other.expiration);
    }

    static final ConstructingObjectParser<CreateApiKeyResponse, Void> PARSER = new ConstructingObjectParser<>("create_api_key_response",
            args -> new CreateApiKeyResponse((String) args[0], (String) args[1], new SecureString((String) args[2]),
                    (args[3] == null) ? null : Instant.ofEpochMilli((Long) args[3])));
    static {
        PARSER.declareString(constructorArg(), new ParseField("name"));
        PARSER.declareString(constructorArg(), new ParseField("id"));
        PARSER.declareString(constructorArg(), new ParseField("api_key"));
        PARSER.declareLong(optionalConstructorArg(), new ParseField("expiration"));
    }

    public static CreateApiKeyResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
