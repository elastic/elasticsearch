/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.action.ActionTypes;
import org.elasticsearch.xpack.core.security.action.privilege.ApplicationPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.profile.UpdateProfileDataAction;
import org.elasticsearch.xpack.core.security.action.profile.UpdateProfileDataRequest;
import org.elasticsearch.xpack.core.security.action.role.BulkDeleteRolesRequest;
import org.elasticsearch.xpack.core.security.action.role.BulkPutRolesRequest;
import org.elasticsearch.xpack.core.security.action.role.DeleteRoleAction;
import org.elasticsearch.xpack.core.security.action.role.DeleteRoleRequest;
import org.elasticsearch.xpack.core.security.action.role.PutRoleAction;
import org.elasticsearch.xpack.core.security.action.role.PutRoleRequest;
import org.elasticsearch.xpack.core.security.authz.RestrictedIndices;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.ClusterPermission;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.IndicesPermission;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege.Category;
import org.elasticsearch.xpack.core.security.support.StringMatcher;
import org.elasticsearch.xpack.core.security.xcontent.XContentUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege.DELETE_INDEX;

/**
 * Static utility class for working with {@link ConfigurableClusterPrivilege} instances
 */
public final class ConfigurableClusterPrivileges {

    public static final ConfigurableClusterPrivilege[] EMPTY_ARRAY = new ConfigurableClusterPrivilege[0];

    private static final Logger logger = LogManager.getLogger(ConfigurableClusterPrivileges.class);
    public static final Writeable.Reader<ConfigurableClusterPrivilege> READER = in1 -> in1.readNamedWriteable(
        ConfigurableClusterPrivilege.class
    );
    public static final Writeable.Writer<ConfigurableClusterPrivilege> WRITER = (out1, value) -> out1.writeNamedWriteable(value);

    private ConfigurableClusterPrivileges() {}

    /**
     * Utility method to read an array of {@link ConfigurableClusterPrivilege} objects from a {@link StreamInput}
     */
    public static ConfigurableClusterPrivilege[] readArray(StreamInput in) throws IOException {
        return in.readArray(READER, ConfigurableClusterPrivilege[]::new);
    }

    /**
     * Utility method to write an array of {@link ConfigurableClusterPrivilege} objects to a {@link StreamOutput}
     */
    public static void writeArray(StreamOutput out, ConfigurableClusterPrivilege[] privileges) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            out.writeArray(WRITER, privileges);
        } else {
            out.writeArray(
                WRITER,
                Arrays.stream(privileges)
                    .filter(privilege -> privilege instanceof ManageRolesPrivilege == false)
                    .toArray(ConfigurableClusterPrivilege[]::new)
            );
        }
    }

    /**
     * Writes a single object value to the {@code builder} that contains each of the provided privileges.
     * The privileges are grouped according to their {@link ConfigurableClusterPrivilege#getCategory() categories}
     */
    public static XContentBuilder toXContent(
        XContentBuilder builder,
        ToXContent.Params params,
        Collection<ConfigurableClusterPrivilege> privileges
    ) throws IOException {
        builder.startObject();
        for (Category category : Category.values()) {
            builder.startObject(category.field.getPreferredName());
            for (ConfigurableClusterPrivilege privilege : privileges) {
                if (category == privilege.getCategory()) {
                    privilege.toXContent(builder, params);
                }
            }
            builder.endObject();
        }
        return builder.endObject();
    }

    /**
     * Read a list of privileges from the parser. The parser should be positioned at the
     * {@link XContentParser.Token#START_OBJECT} token for the privileges value
     */
    public static List<ConfigurableClusterPrivilege> parse(XContentParser parser) throws IOException {
        List<ConfigurableClusterPrivilege> privileges = new ArrayList<>();

        expectedToken(parser.currentToken(), parser, XContentParser.Token.START_OBJECT);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            expectedToken(parser.currentToken(), parser, XContentParser.Token.FIELD_NAME);

            expectFieldName(parser, Category.APPLICATION.field, Category.PROFILE.field, Category.ROLE.field);
            if (Category.APPLICATION.field.match(parser.currentName(), parser.getDeprecationHandler())) {
                expectedToken(parser.nextToken(), parser, XContentParser.Token.START_OBJECT);
                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    expectedToken(parser.currentToken(), parser, XContentParser.Token.FIELD_NAME);

                    expectFieldName(parser, ManageApplicationPrivileges.Fields.MANAGE);
                    privileges.add(ManageApplicationPrivileges.parse(parser));
                }
            } else if (Category.PROFILE.field.match(parser.currentName(), parser.getDeprecationHandler())) {
                expectedToken(parser.nextToken(), parser, XContentParser.Token.START_OBJECT);
                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    expectedToken(parser.currentToken(), parser, XContentParser.Token.FIELD_NAME);

                    expectFieldName(parser, WriteProfileDataPrivileges.Fields.WRITE);
                    privileges.add(WriteProfileDataPrivileges.parse(parser));
                }
            } else if (Category.ROLE.field.match(parser.currentName(), parser.getDeprecationHandler())) {
                expectedToken(parser.nextToken(), parser, XContentParser.Token.START_OBJECT);
                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    expectedToken(parser.currentToken(), parser, XContentParser.Token.FIELD_NAME);

                    expectFieldName(parser, ManageRolesPrivilege.Fields.MANAGE);
                    privileges.add(ManageRolesPrivilege.parse(parser));
                }
            }
        }
        return privileges;
    }

    private static void expectedToken(XContentParser.Token read, XContentParser parser, XContentParser.Token expected) {
        if (read != expected) {
            throw new XContentParseException(
                parser.getTokenLocation(),
                "failed to parse privilege. expected [" + expected + "] but found [" + read + "] instead"
            );
        }
    }

    private static void expectFieldName(XContentParser parser, ParseField... fields) throws IOException {
        final String fieldName = parser.currentName();
        if (Arrays.stream(fields).anyMatch(pf -> pf.match(fieldName, parser.getDeprecationHandler())) == false) {
            throw new XContentParseException(
                parser.getTokenLocation(),
                "failed to parse privilege. expected "
                    + (fields.length == 1 ? "field name" : "one of")
                    + " ["
                    + Strings.arrayToCommaDelimitedString(fields)
                    + "] but found ["
                    + fieldName
                    + "] instead"
            );
        }
    }

    /**
     * The {@link WriteProfileDataPrivileges} privilege is a {@link ConfigurableClusterPrivilege} that grants the
     * ability to write the {@code data} and {@code access} sections of any user profile.
     * The privilege is namespace configurable such that only specific top-level keys in the {@code data} and {@code access}
     * dictionary permit writes (wildcards and regexps are supported, but exclusions are not).
     */
    public static class WriteProfileDataPrivileges implements ConfigurableClusterPrivilege {
        public static final String WRITEABLE_NAME = "write-profile-data-privileges";

        private final Set<String> applicationNames;
        private final Predicate<String> applicationPredicate;
        private final Predicate<TransportRequest> requestPredicate;

        public WriteProfileDataPrivileges(Set<String> applicationNames) {
            this.applicationNames = Collections.unmodifiableSet(applicationNames);
            this.applicationPredicate = StringMatcher.of(applicationNames);
            this.requestPredicate = request -> {
                if (request instanceof final UpdateProfileDataRequest updateProfileRequest) {
                    assert null == updateProfileRequest.validate();
                    final Collection<String> requestApplicationNames = updateProfileRequest.getApplicationNames();
                    return requestApplicationNames.stream().allMatch(applicationPredicate);
                }
                return false;
            };
        }

        @Override
        public Category getCategory() {
            return Category.PROFILE;
        }

        public Collection<String> getApplicationNames() {
            return this.applicationNames;
        }

        @Override
        public String getWriteableName() {
            return WRITEABLE_NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringCollection(this.applicationNames);
        }

        public static WriteProfileDataPrivileges createFrom(StreamInput in) throws IOException {
            final Set<String> applications = in.readCollectionAsSet(StreamInput::readString);
            return new WriteProfileDataPrivileges(applications);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.field(Fields.WRITE.getPreferredName(), Map.of(Fields.APPLICATIONS.getPreferredName(), applicationNames));
        }

        public static WriteProfileDataPrivileges parse(XContentParser parser) throws IOException {
            expectedToken(parser.currentToken(), parser, XContentParser.Token.FIELD_NAME);
            expectFieldName(parser, Fields.WRITE);
            expectedToken(parser.nextToken(), parser, XContentParser.Token.START_OBJECT);
            expectedToken(parser.nextToken(), parser, XContentParser.Token.FIELD_NAME);
            expectFieldName(parser, Fields.APPLICATIONS);
            expectedToken(parser.nextToken(), parser, XContentParser.Token.START_ARRAY);
            final String[] applications = XContentUtils.readStringArray(parser, false);
            expectedToken(parser.nextToken(), parser, XContentParser.Token.END_OBJECT);
            return new WriteProfileDataPrivileges(new LinkedHashSet<>(Arrays.asList(applications)));
        }

        @Override
        public String toString() {
            return "{"
                + getCategory()
                + ":"
                + Fields.WRITE.getPreferredName()
                + ":"
                + Fields.APPLICATIONS.getPreferredName()
                + "="
                + Strings.collectionToDelimitedString(applicationNames, ",")
                + "}";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final WriteProfileDataPrivileges that = (WriteProfileDataPrivileges) o;
            return this.applicationNames.equals(that.applicationNames);
        }

        @Override
        public int hashCode() {
            return applicationNames.hashCode();
        }

        @Override
        public ClusterPermission.Builder buildPermission(ClusterPermission.Builder builder) {
            return builder.add(this, Set.of(UpdateProfileDataAction.NAME), requestPredicate);
        }

        private interface Fields {
            ParseField WRITE = new ParseField("write");
            ParseField APPLICATIONS = new ParseField("applications");
        }
    }

    /**
     * The {@code ManageApplicationPrivileges} privilege is a {@link ConfigurableClusterPrivilege} that grants the
     * ability to execute actions related to the management of application privileges (Get, Put, Delete) for a subset
     * of applications (identified by a wildcard-aware application-name).
     */
    public static class ManageApplicationPrivileges implements ConfigurableClusterPrivilege {
        public static final String WRITEABLE_NAME = "manage-application-privileges";

        private final Set<String> applicationNames;
        private final Predicate<String> applicationPredicate;
        private final Predicate<TransportRequest> requestPredicate;

        public ManageApplicationPrivileges(Set<String> applicationNames) {
            this.applicationNames = Collections.unmodifiableSet(applicationNames);
            this.applicationPredicate = StringMatcher.of(applicationNames);
            this.requestPredicate = request -> {
                if (request instanceof final ApplicationPrivilegesRequest privRequest) {
                    final Collection<String> requestApplicationNames = privRequest.getApplicationNames();
                    return requestApplicationNames.isEmpty()
                        ? this.applicationNames.contains("*")
                        : requestApplicationNames.stream().allMatch(applicationPredicate);
                }
                return false;
            };

        }

        @Override
        public Category getCategory() {
            return Category.APPLICATION;
        }

        public Collection<String> getApplicationNames() {
            return this.applicationNames;
        }

        @Override
        public String getWriteableName() {
            return WRITEABLE_NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringCollection(this.applicationNames);
        }

        public static ManageApplicationPrivileges createFrom(StreamInput in) throws IOException {
            final Set<String> applications = in.readCollectionAsSet(StreamInput::readString);
            return new ManageApplicationPrivileges(applications);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.field(Fields.MANAGE.getPreferredName(), Map.of(Fields.APPLICATIONS.getPreferredName(), applicationNames));
        }

        public static ManageApplicationPrivileges parse(XContentParser parser) throws IOException {
            expectedToken(parser.currentToken(), parser, XContentParser.Token.FIELD_NAME);
            expectFieldName(parser, Fields.MANAGE);
            expectedToken(parser.nextToken(), parser, XContentParser.Token.START_OBJECT);
            expectedToken(parser.nextToken(), parser, XContentParser.Token.FIELD_NAME);
            expectFieldName(parser, Fields.APPLICATIONS);
            expectedToken(parser.nextToken(), parser, XContentParser.Token.START_ARRAY);
            final String[] applications = XContentUtils.readStringArray(parser, false);
            expectedToken(parser.nextToken(), parser, XContentParser.Token.END_OBJECT);
            return new ManageApplicationPrivileges(new LinkedHashSet<>(Arrays.asList(applications)));
        }

        @Override
        public String toString() {
            return "{"
                + getCategory()
                + ":"
                + Fields.MANAGE.getPreferredName()
                + ":"
                + Fields.APPLICATIONS.getPreferredName()
                + "="
                + Strings.collectionToDelimitedString(applicationNames, ",")
                + "}";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final ManageApplicationPrivileges that = (ManageApplicationPrivileges) o;
            return this.applicationNames.equals(that.applicationNames);
        }

        @Override
        public int hashCode() {
            return applicationNames.hashCode();
        }

        @Override
        public ClusterPermission.Builder buildPermission(final ClusterPermission.Builder builder) {
            return builder.add(this, Set.of("cluster:admin/xpack/security/privilege/*"), requestPredicate);
        }

        private interface Fields {
            ParseField MANAGE = new ParseField("manage");
            ParseField APPLICATIONS = new ParseField("applications");
        }
    }

    public static class ManageRolesPrivilege implements ConfigurableClusterPrivilege {
        public static final String WRITEABLE_NAME = "manage-roles-privilege";
        private final List<ManageRolesIndexPermissionGroup> indexPermissionGroups;
        private final Function<RestrictedIndices, Predicate<TransportRequest>> requestPredicateSupplier;

        private static final Set<String> EXPECTED_INDEX_GROUP_FIELDS = Set.of(
            Fields.NAMES.getPreferredName(),
            Fields.PRIVILEGES.getPreferredName()
        );

        public ManageRolesPrivilege(List<ManageRolesIndexPermissionGroup> manageRolesIndexPermissionGroups) {
            this.indexPermissionGroups = manageRolesIndexPermissionGroups;
            this.requestPredicateSupplier = (restrictedIndices) -> {
                IndicesPermission.Builder indicesPermissionBuilder = new IndicesPermission.Builder(restrictedIndices);
                for (ManageRolesIndexPermissionGroup indexPatternPrivilege : manageRolesIndexPermissionGroups) {
                    indicesPermissionBuilder.addGroup(
                        IndexPrivilege.get(Set.of(indexPatternPrivilege.privileges())),
                        FieldPermissions.DEFAULT,
                        null,
                        false,
                        indexPatternPrivilege.indexPatterns()
                    );
                }
                final IndicesPermission indicesPermission = indicesPermissionBuilder.build();

                return (TransportRequest request) -> {
                    if (request instanceof final PutRoleRequest putRoleRequest) {
                        return hasNonIndexPrivileges(putRoleRequest.roleDescriptor()) == false
                            && Arrays.stream(putRoleRequest.indices())
                                .noneMatch(
                                    indexPrivilege -> requestIndexPatternsAllowed(
                                        indicesPermission,
                                        indexPrivilege.getIndices(),
                                        indexPrivilege.getPrivileges()
                                    ) == false
                                );
                    } else if (request instanceof final BulkPutRolesRequest bulkPutRoleRequest) {
                        return bulkPutRoleRequest.getRoles().stream().noneMatch(ManageRolesPrivilege::hasNonIndexPrivileges)
                            && bulkPutRoleRequest.getRoles()
                                .stream()
                                .allMatch(
                                    roleDescriptor -> Arrays.stream(roleDescriptor.getIndicesPrivileges())
                                        .noneMatch(
                                            indexPrivilege -> requestIndexPatternsAllowed(
                                                indicesPermission,
                                                indexPrivilege.getIndices(),
                                                indexPrivilege.getPrivileges()
                                            ) == false
                                        )
                                );
                    } else if (request instanceof final DeleteRoleRequest deleteRoleRequest) {
                        return requestIndexPatternsAllowed(
                            indicesPermission,
                            new String[] { deleteRoleRequest.name() },
                            DELETE_INDEX.name().toArray(String[]::new)
                        );
                    } else if (request instanceof final BulkDeleteRolesRequest bulkDeleteRoleRequest) {
                        return requestIndexPatternsAllowed(
                            indicesPermission,
                            bulkDeleteRoleRequest.getRoleNames().toArray(String[]::new),
                            DELETE_INDEX.name().toArray(String[]::new)
                        );
                    }
                    throw new IllegalArgumentException("Unsupported request type [" + request.getClass() + "]");
                };
            };
        }

        @Override
        public Category getCategory() {
            return Category.ROLE;
        }

        @Override
        public String getWriteableName() {
            return WRITEABLE_NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(indexPermissionGroups);
        }

        public static ManageRolesPrivilege createFrom(StreamInput in) throws IOException {
            final List<ManageRolesIndexPermissionGroup> indexPatternPrivileges = in.readCollectionAsList(
                ManageRolesIndexPermissionGroup::createFrom
            );
            return new ManageRolesPrivilege(indexPatternPrivileges);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.field(
                Fields.MANAGE.getPreferredName(),
                Map.of(Fields.INDICES.getPreferredName(), indexPermissionGroups.stream().map(indexPatternPrivilege -> {
                    Map<String, String[]> sortedMap = new TreeMap<>();
                    sortedMap.put(Fields.NAMES.getPreferredName(), indexPatternPrivilege.indexPatterns());
                    sortedMap.put(Fields.PRIVILEGES.getPreferredName(), indexPatternPrivilege.privileges());
                    return sortedMap;
                }).toList())
            );
        }

        private static void expectedIndexGroupFields(String fieldName, XContentParser parser) {
            if (EXPECTED_INDEX_GROUP_FIELDS.contains(fieldName) == false) {
                throw new XContentParseException(
                    parser.getTokenLocation(),
                    "failed to parse privilege. expected one of "
                        + Arrays.toString(EXPECTED_INDEX_GROUP_FIELDS.toArray(String[]::new))
                        + " but found ["
                        + fieldName
                        + "] instead"
                );
            }
        }

        public static ManageRolesPrivilege parse(XContentParser parser) throws IOException {
            expectedToken(parser.currentToken(), parser, XContentParser.Token.FIELD_NAME);
            expectFieldName(parser, Fields.MANAGE);
            expectedToken(parser.nextToken(), parser, XContentParser.Token.START_OBJECT);
            expectedToken(parser.nextToken(), parser, XContentParser.Token.FIELD_NAME);
            expectFieldName(parser, Fields.INDICES);
            expectedToken(parser.nextToken(), parser, XContentParser.Token.START_ARRAY);
            List<ManageRolesIndexPermissionGroup> indexPrivileges = new ArrayList<>();
            Map<String, String[]> parsedArraysByFieldName = new HashMap<>();

            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                expectedToken(token, parser, XContentParser.Token.START_OBJECT);
                expectedToken(parser.nextToken(), parser, XContentParser.Token.FIELD_NAME);
                String currentFieldName = parser.currentName();
                expectedIndexGroupFields(currentFieldName, parser);
                expectedToken(parser.nextToken(), parser, XContentParser.Token.START_ARRAY);
                parsedArraysByFieldName.put(currentFieldName, XContentUtils.readStringArray(parser, false));
                expectedToken(parser.nextToken(), parser, XContentParser.Token.FIELD_NAME);
                currentFieldName = parser.currentName();
                expectedIndexGroupFields(currentFieldName, parser);
                expectedToken(parser.nextToken(), parser, XContentParser.Token.START_ARRAY);
                parsedArraysByFieldName.put(currentFieldName, XContentUtils.readStringArray(parser, false));
                expectedToken(parser.nextToken(), parser, XContentParser.Token.END_OBJECT);
                indexPrivileges.add(
                    new ManageRolesIndexPermissionGroup(
                        parsedArraysByFieldName.get(Fields.NAMES.getPreferredName()),
                        parsedArraysByFieldName.get(Fields.PRIVILEGES.getPreferredName())
                    )
                );
            }
            expectedToken(parser.nextToken(), parser, XContentParser.Token.END_OBJECT);

            for (var indexPrivilege : indexPrivileges) {
                if (indexPrivilege.indexPatterns == null || indexPrivilege.indexPatterns.length == 0) {
                    throw new IllegalArgumentException("Indices privileges must refer to at least one index name or index name pattern");
                }
                if (indexPrivilege.privileges == null || indexPrivilege.privileges.length == 0) {
                    throw new IllegalArgumentException("Indices privileges must define at least one privilege");
                }

                for (String privilege : indexPrivilege.privileges) {
                    // Use resolveBySelectorAccess to determine whether the passed privilege is valid.
                    // IllegalArgumentException is thrown here when an invalid permission is encountered.
                    IndexPrivilege.get(Set.of(privilege));
                }

            }
            return new ManageRolesPrivilege(indexPrivileges);
        }

        public record ManageRolesIndexPermissionGroup(String[] indexPatterns, String[] privileges) implements Writeable {
            public static ManageRolesIndexPermissionGroup createFrom(StreamInput in) throws IOException {
                return new ManageRolesIndexPermissionGroup(in.readStringArray(), in.readStringArray());
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeStringArray(indexPatterns);
                out.writeStringArray(privileges);
            }

            @Override
            public String toString() {
                return "{"
                    + Fields.NAMES
                    + ":"
                    + Arrays.toString(indexPatterns())
                    + ":"
                    + Fields.PRIVILEGES
                    + ":"
                    + Arrays.toString(privileges())
                    + "}";
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                ManageRolesIndexPermissionGroup that = (ManageRolesIndexPermissionGroup) o;
                return Arrays.equals(indexPatterns, that.indexPatterns) && Arrays.equals(privileges, that.privileges);
            }

            @Override
            public int hashCode() {
                return Objects.hash(Arrays.hashCode(indexPatterns), Arrays.hashCode(privileges));
            }
        }

        @Override
        public String toString() {
            return "{"
                + getCategory()
                + ":"
                + Fields.MANAGE.getPreferredName()
                + ":"
                + Fields.INDICES.getPreferredName()
                + "=["
                + Strings.collectionToDelimitedString(indexPermissionGroups, ",")
                + "]}";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final ManageRolesPrivilege that = (ManageRolesPrivilege) o;

            if (this.indexPermissionGroups.size() != that.indexPermissionGroups.size()) {
                return false;
            }

            for (int i = 0; i < this.indexPermissionGroups.size(); i++) {
                if (Objects.equals(this.indexPermissionGroups.get(i), that.indexPermissionGroups.get(i)) == false) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexPermissionGroups.hashCode());
        }

        @Override
        public ClusterPermission.Builder buildPermission(final ClusterPermission.Builder builder) {
            return builder.addWithPredicateSupplier(
                this,
                Set.of(PutRoleAction.NAME, ActionTypes.BULK_PUT_ROLES.name(), ActionTypes.BULK_DELETE_ROLES.name(), DeleteRoleAction.NAME),
                requestPredicateSupplier
            );
        }

        private static boolean requestIndexPatternsAllowed(
            IndicesPermission indicesPermission,
            String[] requestIndexPatterns,
            String[] privileges
        ) {
            return indicesPermission.checkResourcePrivileges(Set.of(requestIndexPatterns), false, Set.of(privileges), true, null);
        }

        private static boolean hasNonIndexPrivileges(RoleDescriptor roleDescriptor) {
            return roleDescriptor.hasApplicationPrivileges()
                || roleDescriptor.hasClusterPrivileges()
                || roleDescriptor.hasConfigurableClusterPrivileges()
                || roleDescriptor.hasRemoteIndicesPrivileges()
                || roleDescriptor.hasRemoteClusterPermissions()
                || roleDescriptor.hasRunAs()
                || roleDescriptor.hasWorkflowsRestriction();
        }

        private interface Fields {
            ParseField MANAGE = new ParseField("manage");
            ParseField INDICES = new ParseField("indices");
            ParseField PRIVILEGES = new ParseField("privileges");
            ParseField NAMES = new ParseField("names");
        }
    }
}
