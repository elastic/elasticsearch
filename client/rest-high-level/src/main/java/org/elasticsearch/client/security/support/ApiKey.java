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

package org.elasticsearch.client.security.support;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * API key information
 */
public final class ApiKey {

    private final String name;
    private final String id;
    private final Instant creation;
    private final Instant expiration;
    private final boolean invalidated;
    private final String username;
    private final String realm;

    public ApiKey(String name, String id, Instant creation, Instant expiration, boolean invalidated, String username, String realm) {
        this.name = name;
        this.id = id;
        // As we do not yet support the nanosecond precision when we serialize to JSON,
        // here creating the 'Instant' of milliseconds precision.
        // This Instant can then be used for date comparison.
        this.creation = Instant.ofEpochMilli(creation.toEpochMilli());
        this.expiration = (expiration != null) ? Instant.ofEpochMilli(expiration.toEpochMilli()): null;
        this.invalidated = invalidated;
        this.username = username;
        this.realm = realm;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    /**
     * @return a instance of {@link Instant} when this API key was created.
     */
    public Instant getCreation() {
        return creation;
    }

    /**
     * @return a instance of {@link Instant} when this API key will expire. In case the API key does not expire then will return
     * {@code null}
     */
    public Instant getExpiration() {
        return expiration;
    }

    /**
     * @return {@code true} if this API key has been invalidated else returns {@code false}
     */
    public boolean isInvalidated() {
        return invalidated;
    }

    /**
     * @return the username for which this API key was created.
     */
    public String getUsername() {
        return username;
    }

    /**
     * @return the realm name of the user for which this API key was created.
     */
    public String getRealm() {
        return realm;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, id, creation, expiration, invalidated, username, realm);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ApiKey other = (ApiKey) obj;
        return Objects.equals(name, other.name)
                && Objects.equals(id, other.id)
                && Objects.equals(creation, other.creation)
                && Objects.equals(expiration, other.expiration)
                && Objects.equals(invalidated, other.invalidated)
                && Objects.equals(username, other.username)
                && Objects.equals(realm, other.realm);
    }

    static ConstructingObjectParser<ApiKey, Void> PARSER = new ConstructingObjectParser<>("api_key", args -> {
        return new ApiKey((String) args[0], (String) args[1], Instant.ofEpochMilli((Long) args[2]),
                (args[3] == null) ? null : Instant.ofEpochMilli((Long) args[3]), (Boolean) args[4], (String) args[5], (String) args[6]);
    });
    static {
        PARSER.declareString(constructorArg(), new ParseField("name"));
        PARSER.declareString(constructorArg(), new ParseField("id"));
        PARSER.declareLong(constructorArg(), new ParseField("creation"));
        PARSER.declareLong(optionalConstructorArg(), new ParseField("expiration"));
        PARSER.declareBoolean(constructorArg(), new ParseField("invalidated"));
        PARSER.declareString(constructorArg(), new ParseField("username"));
        PARSER.declareString(constructorArg(), new ParseField("realm"));
    }

    public static ApiKey fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public String toString() {
        return "ApiKey [name=" + name + ", id=" + id + ", creation=" + creation + ", expiration=" + expiration + ", invalidated="
                + invalidated + ", username=" + username + ", realm=" + realm + "]";
    }
}
