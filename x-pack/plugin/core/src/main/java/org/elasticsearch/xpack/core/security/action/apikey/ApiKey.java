/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.action.admin.cluster.node.info.ComponentVersionNumber;
import org.elasticsearch.common.VersionId;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.AbstractObjectParser;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder.CCR_INDICES_PRIVILEGE_NAMES;
import static org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder.CCS_INDICES_PRIVILEGE_NAMES;

/**
 * API key information
 */
public final class ApiKey implements ToXContentObject {

    public enum Type {
        /**
         * REST type API keys can authenticate on the HTTP interface
         */
        REST,
        /**
         * Cross cluster type API keys can authenticate on the dedicated remote cluster server interface
         */
        CROSS_CLUSTER;

        public static Type parse(String value) {
            return switch (value.toLowerCase(Locale.ROOT)) {
                case "rest" -> REST;
                case "cross_cluster" -> CROSS_CLUSTER;
                default -> throw new IllegalArgumentException(
                    "invalid API key type ["
                        + value
                        + "] expected one of ["
                        + Stream.of(values()).map(Type::value).collect(Collectors.joining(","))
                        + "]"
                );
            };
        }

        public static Type fromXContent(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.currentToken();
            if (token == null) {
                token = parser.nextToken();
            }
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_STRING, token, parser);
            return parse(parser.text());
        }

        public String value() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public record Version(int version) implements VersionId<Version> {
        @Override
        public int id() {
            return version;
        }
    }

    public static class VersionComponent implements ComponentVersionNumber {

        @Override
        public String componentId() {
            return "api_key_version";
        }

        @Override
        public VersionId<?> versionNumber() {
            return CURRENT_API_KEY_VERSION;
        }
    }

    public static final ApiKey.Version CURRENT_API_KEY_VERSION = new ApiKey.Version(8_15_00_99);

    private final String name;
    private final String id;
    private final Type type;
    private final Instant creation;
    private final Instant expiration;
    private final boolean invalidated;
    private final Instant invalidation;
    private final String username;
    private final String realm;
    @Nullable
    private final String realmType;
    private final Map<String, Object> metadata;
    @Nullable
    private final List<RoleDescriptor> roleDescriptors;
    @Nullable
    private final RoleDescriptorsIntersection limitedBy;

    public ApiKey(
        String name,
        String id,
        Type type,
        Instant creation,
        Instant expiration,
        boolean invalidated,
        @Nullable Instant invalidation,
        String username,
        String realm,
        @Nullable String realmType,
        @Nullable Map<String, Object> metadata,
        @Nullable List<RoleDescriptor> roleDescriptors,
        @Nullable List<RoleDescriptor> limitedByRoleDescriptors
    ) {
        this(
            name,
            id,
            type,
            creation,
            expiration,
            invalidated,
            invalidation,
            username,
            realm,
            realmType,
            metadata,
            roleDescriptors,
            limitedByRoleDescriptors == null ? null : new RoleDescriptorsIntersection(List.of(Set.copyOf(limitedByRoleDescriptors)))
        );
    }

    private ApiKey(
        String name,
        String id,
        Type type,
        Instant creation,
        Instant expiration,
        boolean invalidated,
        Instant invalidation,
        String username,
        String realm,
        @Nullable String realmType,
        @Nullable Map<String, Object> metadata,
        @Nullable List<RoleDescriptor> roleDescriptors,
        @Nullable RoleDescriptorsIntersection limitedBy
    ) {
        this.name = name;
        this.id = id;
        this.type = type;
        // As we do not yet support the nanosecond precision when we serialize to JSON,
        // here creating the 'Instant' of milliseconds precision.
        // This Instant can then be used for date comparison.
        this.creation = Instant.ofEpochMilli(creation.toEpochMilli());
        this.expiration = (expiration != null) ? Instant.ofEpochMilli(expiration.toEpochMilli()) : null;
        this.invalidated = invalidated;
        this.invalidation = (invalidation != null) ? Instant.ofEpochMilli(invalidation.toEpochMilli()) : null;
        this.username = username;
        this.realm = realm;
        this.realmType = realmType;
        this.metadata = metadata == null ? Map.of() : metadata;
        this.roleDescriptors = roleDescriptors != null ? List.copyOf(roleDescriptors) : null;
        // This assertion will need to be changed (or removed) when derived keys are properly supported
        assert limitedBy == null || limitedBy.roleDescriptorsList().size() == 1 : "can only have one set of limited-by role descriptors";
        this.limitedBy = limitedBy;
    }

    // Should only be used by XContent parsers
    @SuppressWarnings("unchecked")
    ApiKey(Object[] parsed) {
        this(
            (String) parsed[0],
            (String) parsed[1],
            (Type) parsed[2],
            Instant.ofEpochMilli((Long) parsed[3]),
            (parsed[4] == null) ? null : Instant.ofEpochMilli((Long) parsed[4]),
            (Boolean) parsed[5],
            (parsed[6] == null) ? null : Instant.ofEpochMilli((Long) parsed[6]),
            (String) parsed[7],
            (String) parsed[8],
            (String) parsed[9],
            (parsed[10] == null) ? null : (Map<String, Object>) parsed[10],
            (List<RoleDescriptor>) parsed[11],
            (RoleDescriptorsIntersection) parsed[12]
        );
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Type getType() {
        return type;
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

    public Instant getInvalidation() {
        return invalidation;
    }

    public String getUsername() {
        return username;
    }

    public String getRealm() {
        return realm;
    }

    public @Nullable String getRealmType() {
        return realmType;
    }

    public @Nullable RealmConfig.RealmIdentifier getRealmIdentifier() {
        if (realm != null && realmType != null) {
            return new RealmConfig.RealmIdentifier(realmType, realm);
        }
        return null;
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
        builder.field("id", id).field("name", name);
        builder.field("type", type.value());
        builder.field("creation", creation.toEpochMilli());
        if (expiration != null) {
            builder.field("expiration", expiration.toEpochMilli());
        }
        builder.field("invalidated", invalidated);
        if (invalidation != null) {
            builder.field("invalidation", invalidation.toEpochMilli());
        }
        builder.field("username", username).field("realm", realm);
        if (realmType != null) {
            builder.field("realm_type", realmType);
        }
        builder.field("metadata", (metadata == null ? Map.of() : metadata));
        if (roleDescriptors != null) {
            builder.startObject("role_descriptors");
            for (var roleDescriptor : roleDescriptors) {
                builder.field(roleDescriptor.getName(), roleDescriptor);
            }
            builder.endObject();
            if (type == Type.CROSS_CLUSTER) {
                assert roleDescriptors.size() == 1;
                buildXContentForCrossClusterApiKeyAccess(builder, roleDescriptors.iterator().next());
            }
        }
        if (limitedBy != null) {
            assert type != Type.CROSS_CLUSTER;
            builder.field("limited_by", limitedBy);
        }
        return builder;
    }

    private void buildXContentForCrossClusterApiKeyAccess(XContentBuilder builder, RoleDescriptor roleDescriptor) throws IOException {
        if (Assertions.ENABLED) {
            CrossClusterApiKeyRoleDescriptorBuilder.validate(roleDescriptor);
        }
        final List<RoleDescriptor.IndicesPrivileges> search = new ArrayList<>();
        final List<RoleDescriptor.IndicesPrivileges> replication = new ArrayList<>();
        for (RoleDescriptor.IndicesPrivileges indicesPrivileges : roleDescriptor.getIndicesPrivileges()) {
            if (Arrays.equals(CCS_INDICES_PRIVILEGE_NAMES, indicesPrivileges.getPrivileges())) {
                search.add(indicesPrivileges);
            } else {
                assert Arrays.equals(CCR_INDICES_PRIVILEGE_NAMES, indicesPrivileges.getPrivileges());
                replication.add(indicesPrivileges);
            }
        }
        builder.startObject("access");
        final Params params = new MapParams(Map.of("_with_privileges", "false"));
        if (false == search.isEmpty()) {
            builder.startArray("search");
            for (RoleDescriptor.IndicesPrivileges indicesPrivileges : search) {
                indicesPrivileges.toXContent(builder, params);
            }
            builder.endArray();
        }
        if (false == replication.isEmpty()) {
            builder.startArray("replication");
            for (RoleDescriptor.IndicesPrivileges indicesPrivileges : replication) {
                indicesPrivileges.toXContent(builder, params);
            }
            builder.endArray();
        }
        builder.endObject();
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            name,
            id,
            type,
            creation,
            expiration,
            invalidated,
            invalidation,
            username,
            realm,
            realmType,
            metadata,
            roleDescriptors,
            limitedBy
        );
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
            && Objects.equals(type, other.type)
            && Objects.equals(creation, other.creation)
            && Objects.equals(expiration, other.expiration)
            && Objects.equals(invalidated, other.invalidated)
            && Objects.equals(invalidation, other.invalidation)
            && Objects.equals(username, other.username)
            && Objects.equals(realm, other.realm)
            && Objects.equals(realmType, other.realmType)
            && Objects.equals(metadata, other.metadata)
            && Objects.equals(roleDescriptors, other.roleDescriptors)
            && Objects.equals(limitedBy, other.limitedBy);
    }

    @Override
    public String toString() {
        return "ApiKey [name="
            + name
            + ", id="
            + id
            + ", type="
            + type.value()
            + ", creation="
            + creation
            + ", expiration="
            + expiration
            + ", invalidated="
            + invalidated
            + ", invalidation="
            + invalidation
            + ", username="
            + username
            + ", realm="
            + realm
            + ", realm_type="
            + realmType
            + ", metadata="
            + metadata
            + ", role_descriptors="
            + roleDescriptors
            + ", limited_by="
            + limitedBy
            + "]";
    }

    private static final RoleDescriptor.Parser ROLE_DESCRIPTOR_PARSER = RoleDescriptor.parserBuilder().allowRestriction(true).build();
    static final ConstructingObjectParser<ApiKey, Void> PARSER;
    static {
        PARSER = new ConstructingObjectParser<>("api_key", true, ApiKey::new);
        initializeParser(PARSER);
    }

    public static ApiKey fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    static int initializeParser(AbstractObjectParser<?, Void> parser) {
        parser.declareString(constructorArg(), new ParseField("name"));
        parser.declareString(constructorArg(), new ParseField("id"));
        parser.declareField(constructorArg(), Type::fromXContent, new ParseField("type"), ObjectParser.ValueType.STRING);
        parser.declareLong(constructorArg(), new ParseField("creation"));
        parser.declareLong(optionalConstructorArg(), new ParseField("expiration"));
        parser.declareBoolean(constructorArg(), new ParseField("invalidated"));
        parser.declareLong(optionalConstructorArg(), new ParseField("invalidation"));
        parser.declareString(constructorArg(), new ParseField("username"));
        parser.declareString(constructorArg(), new ParseField("realm"));
        parser.declareStringOrNull(optionalConstructorArg(), new ParseField("realm_type"));
        parser.declareObject(optionalConstructorArg(), (p, c) -> p.map(), new ParseField("metadata"));
        parser.declareNamedObjects(optionalConstructorArg(), (p, c, n) -> {
            p.nextToken();
            return ROLE_DESCRIPTOR_PARSER.parse(n, p);
        }, new ParseField("role_descriptors"));
        parser.declareField(
            optionalConstructorArg(),
            (p, c) -> RoleDescriptorsIntersection.fromXContent(p),
            new ParseField("limited_by"),
            ObjectParser.ValueType.OBJECT_ARRAY
        );
        return 13; // the number of fields to parse
    }
}
