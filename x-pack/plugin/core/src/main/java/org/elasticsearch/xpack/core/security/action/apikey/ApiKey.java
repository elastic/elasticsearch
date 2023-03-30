/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * API key information
 */
public final class ApiKey implements ToXContentObject, Writeable {

    private final String name;
    private final String id;
    private final Instant creation;
    private final Instant expiration;
    private final boolean invalidated;
    private final String username;
    private final String realm;
    private final Map<String, Object> metadata;
    @Nullable
    private final List<RoleDescriptor> roleDescriptors;
    @Nullable
    private final RoleDescriptorsIntersection limitedBy;

    public ApiKey(
        String name,
        String id,
        Instant creation,
        Instant expiration,
        boolean invalidated,
        String username,
        String realm,
        @Nullable Map<String, Object> metadata,
        @Nullable List<RoleDescriptor> roleDescriptors,
        @Nullable List<RoleDescriptor> limitedByRoleDescriptors
    ) {
        this(
            name,
            id,
            creation,
            expiration,
            invalidated,
            username,
            realm,
            metadata,
            roleDescriptors,
            limitedByRoleDescriptors == null ? null : new RoleDescriptorsIntersection(List.of(Set.copyOf(limitedByRoleDescriptors)))
        );
    }

    private ApiKey(
        String name,
        String id,
        Instant creation,
        Instant expiration,
        boolean invalidated,
        String username,
        String realm,
        @Nullable Map<String, Object> metadata,
        @Nullable List<RoleDescriptor> roleDescriptors,
        @Nullable RoleDescriptorsIntersection limitedBy
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
        this.metadata = metadata == null ? Map.of() : metadata;
        this.roleDescriptors = roleDescriptors != null ? List.copyOf(roleDescriptors) : null;
        // This assertion will need to be changed (or removed) when derived keys are properly supported
        assert limitedBy == null || limitedBy.roleDescriptorsList().size() == 1 : "can only have one set of limited-by role descriptors";
        this.limitedBy = limitedBy;
    }

    public ApiKey(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_7_5_0)) {
            this.name = in.readOptionalString();
        } else {
            this.name = in.readString();
        }
        this.id = in.readString();
        this.creation = in.readInstant();
        this.expiration = in.readOptionalInstant();
        this.invalidated = in.readBoolean();
        this.username = in.readString();
        this.realm = in.readString();
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_0_0)) {
            this.metadata = in.readMap();
        } else {
            this.metadata = Map.of();
        }
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_5_0)) {
            final List<RoleDescriptor> roleDescriptors = in.readOptionalList(RoleDescriptor::new);
            this.roleDescriptors = roleDescriptors != null ? List.copyOf(roleDescriptors) : null;
            this.limitedBy = in.readOptionalWriteable(RoleDescriptorsIntersection::new);
        } else {
            this.roleDescriptors = null;
            this.limitedBy = null;
        }
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Instant getCreation() {
        return creation;
    }

    public Instant getExpiration() {
        return expiration;
    }

    public boolean isInvalidated() {
        return invalidated;
    }

    public String getUsername() {
        return username;
    }

    public String getRealm() {
        return realm;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public List<RoleDescriptor> getRoleDescriptors() {
        return roleDescriptors;
    }

    public RoleDescriptorsIntersection getLimitedBy() {
        return limitedBy;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        innerToXContent(builder, params);
        return builder.endObject();
    }

    public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("id", id).field("name", name).field("creation", creation.toEpochMilli());
        if (expiration != null) {
            builder.field("expiration", expiration.toEpochMilli());
        }
        builder.field("invalidated", invalidated)
            .field("username", username)
            .field("realm", realm)
            .field("metadata", (metadata == null ? Map.of() : metadata));
        if (roleDescriptors != null) {
            builder.startObject("role_descriptors");
            for (var roleDescriptor : roleDescriptors) {
                builder.field(roleDescriptor.getName(), roleDescriptor);
            }
            builder.endObject();
        }
        if (limitedBy != null) {
            builder.field("limited_by", limitedBy);
        }
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_7_5_0)) {
            out.writeOptionalString(name);
        } else {
            out.writeString(name);
        }
        out.writeString(id);
        out.writeInstant(creation);
        out.writeOptionalInstant(expiration);
        out.writeBoolean(invalidated);
        out.writeString(username);
        out.writeString(realm);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_0_0)) {
            out.writeGenericMap(metadata);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_5_0)) {
            out.writeOptionalCollection(roleDescriptors);
            out.writeOptionalWriteable(limitedBy);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, id, creation, expiration, invalidated, username, realm, metadata, roleDescriptors, limitedBy);
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
            && Objects.equals(roleDescriptors, other.roleDescriptors)
            && Objects.equals(limitedBy, other.limitedBy);
    }

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<ApiKey, Void> PARSER = new ConstructingObjectParser<>("api_key", args -> {
        return new ApiKey(
            (String) args[0],
            (String) args[1],
            Instant.ofEpochMilli((Long) args[2]),
            (args[3] == null) ? null : Instant.ofEpochMilli((Long) args[3]),
            (Boolean) args[4],
            (String) args[5],
            (String) args[6],
            (args[7] == null) ? null : (Map<String, Object>) args[7],
            (List<RoleDescriptor>) args[8],
            (RoleDescriptorsIntersection) args[9]
        );
    });
    static {
        PARSER.declareString(constructorArg(), new ParseField("name"));
        PARSER.declareString(constructorArg(), new ParseField("id"));
        PARSER.declareLong(constructorArg(), new ParseField("creation"));
        PARSER.declareLong(optionalConstructorArg(), new ParseField("expiration"));
        PARSER.declareBoolean(constructorArg(), new ParseField("invalidated"));
        PARSER.declareString(constructorArg(), new ParseField("username"));
        PARSER.declareString(constructorArg(), new ParseField("realm"));
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.map(), new ParseField("metadata"));
        PARSER.declareNamedObjects(optionalConstructorArg(), (p, c, n) -> {
            p.nextToken();
            return RoleDescriptor.parse(n, p, false);
        }, new ParseField("role_descriptors"));
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> RoleDescriptorsIntersection.fromXContent(p),
            new ParseField("limited_by"),
            ObjectParser.ValueType.OBJECT_ARRAY
        );
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
            + ", metadata="
            + metadata
            + ", role_descriptors="
            + roleDescriptors
            + ", limited_by="
            + limitedBy
            + "]";
    }

}
