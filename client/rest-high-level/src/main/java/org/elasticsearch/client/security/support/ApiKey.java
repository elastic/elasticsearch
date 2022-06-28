/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security.support;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

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
    private final Map<String, Object> metadata;
    @Nullable
    private final Object[] sortValues;

    public ApiKey(
        String name,
        String id,
        Instant creation,
        Instant expiration,
        boolean invalidated,
        String username,
        String realm,
        Map<String, Object> metadata
    ) {
        this(name, id, creation, expiration, invalidated, username, realm, metadata, null);
    }

    public ApiKey(
        String name,
        String id,
        Instant creation,
        Instant expiration,
        boolean invalidated,
        String username,
        String realm,
        Map<String, Object> metadata,
        @Nullable Object[] sortValues
    ) {
        this.name = name;
        this.id = id;
        // As we do not yet support the nanosecond precision when we serialize to JSON,
        // here creating the 'Instant' of milliseconds precision.
        // This Instant can then be used for date comparison.
        this.creation = Instant.ofEpochMilli(creation.toEpochMilli());
        this.expiration = (expiration != null) ? Instant.ofEpochMilli(expiration.toEpochMilli()) : null;
        this.invalidated = invalidated;
        this.username = username;
        this.realm = realm;
        this.metadata = metadata;
        this.sortValues = sortValues;
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

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    /**
     * API keys can be retrieved with either {@link org.elasticsearch.client.security.GetApiKeyRequest}
     * or {@link org.elasticsearch.client.security.QueryApiKeyRequest}. When sorting is specified for
     * QueryApiKeyRequest, the sort values for each key is returned along with each API key.
     *
     * @return Sort values for this API key if it is retrieved with QueryApiKeyRequest and sorting is
     *         required. Otherwise, it is null.
     */
    public Object[] getSortValues() {
        return sortValues;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, id, creation, expiration, invalidated, username, realm, metadata, Arrays.hashCode(sortValues));
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
            && Objects.equals(realm, other.realm)
            && Objects.equals(metadata, other.metadata)
            && Arrays.equals(sortValues, other.sortValues);
    }

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<ApiKey, Void> PARSER = new ConstructingObjectParser<>("api_key", args -> {
        final Object[] sortValues;
        if (args[8] == null) {
            sortValues = null;
        } else {
            final List<Object> arg8 = (List<Object>) args[8];
            sortValues = arg8.isEmpty() ? null : arg8.toArray();
        }
        return new ApiKey(
            (String) args[0],
            (String) args[1],
            Instant.ofEpochMilli((Long) args[2]),
            (args[3] == null) ? null : Instant.ofEpochMilli((Long) args[3]),
            (Boolean) args[4],
            (String) args[5],
            (String) args[6],
            (Map<String, Object>) args[7],
            sortValues
        );
    });
    static {
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> p.textOrNull(),
            new ParseField("name"),
            ObjectParser.ValueType.STRING_OR_NULL
        );
        PARSER.declareString(constructorArg(), new ParseField("id"));
        PARSER.declareLong(constructorArg(), new ParseField("creation"));
        PARSER.declareLong(optionalConstructorArg(), new ParseField("expiration"));
        PARSER.declareBoolean(constructorArg(), new ParseField("invalidated"));
        PARSER.declareString(constructorArg(), new ParseField("username"));
        PARSER.declareString(constructorArg(), new ParseField("realm"));
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.map(), new ParseField("metadata"));
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> p.objectText(), new ParseField("_sort"));
    }

    public static ApiKey fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public String toString() {
        return "ApiKey [name="
            + name
            + ", id="
            + id
            + ", creation="
            + creation
            + ", expiration="
            + expiration
            + ", invalidated="
            + invalidated
            + ", username="
            + username
            + ", realm="
            + realm
            + ", _sort="
            + Arrays.toString(sortValues)
            + "]";
    }
}
