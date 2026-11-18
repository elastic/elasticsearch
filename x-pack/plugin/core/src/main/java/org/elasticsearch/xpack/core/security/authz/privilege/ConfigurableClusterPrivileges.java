/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.esql.DataSourceRequestInfo;
import org.elasticsearch.xpack.core.esql.EsqlDataSourceActionNames;
import org.elasticsearch.xpack.core.esql.EsqlDatasetActionNames;
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
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateUserManagedServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.DeleteUserManagedServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.DeleteUserManagedServiceAccountRequest;
import org.elasticsearch.xpack.core.security.action.service.DeleteUserManagedServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsAction;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsRequest;
import org.elasticsearch.xpack.core.security.action.service.PutUserManagedServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.PutUserManagedServiceAccountRequest;
import org.elasticsearch.xpack.core.security.action.service.QueryServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.ServiceAccountReadRequest;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccount.ServiceAccountId;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings;
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
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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
        out.writeArray(WRITER, privileges);
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
            if (category == Category.SERVICE_ACCOUNT && privileges.stream().noneMatch(p -> p.getCategory() == Category.SERVICE_ACCOUNT)) {
                // Written only when present: an empty object is a field that older nodes cannot parse and that the strict
                // security index mapping rejects until it is upgraded.
                continue;
            }
            if (category == Category.DATASOURCE) {
                builder.startArray(category.field.getPreferredName());
                for (ConfigurableClusterPrivilege privilege : privileges) {
                    if (privilege instanceof DatasourcePrivileges mds) {
                        mds.toXContentArrayElements(builder, params);
                    }
                }
                builder.endArray();
            } else {
                builder.startObject(category.field.getPreferredName());
                for (ConfigurableClusterPrivilege privilege : privileges) {
                    if (category == privilege.getCategory()) {
                        privilege.toXContent(builder, params);
                    }
                }
                builder.endObject();
            }
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

            expectFieldName(
                parser,
                Category.APPLICATION.field,
                Category.PROFILE.field,
                Category.ROLE.field,
                Category.DATASOURCE.field,
                Category.SERVICE_ACCOUNT.field
            );
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
            } else if (Category.DATASOURCE.field.match(parser.currentName(), parser.getDeprecationHandler())) {
                expectedToken(parser.nextToken(), parser, XContentParser.Token.START_ARRAY);
                DatasourcePrivileges datasourcePrivileges = DatasourcePrivileges.parseArray(parser);
                if (datasourcePrivileges != null) {
                    privileges.add(datasourcePrivileges);
                }
            } else if (Category.SERVICE_ACCOUNT.field.match(parser.currentName(), parser.getDeprecationHandler())) {
                expectedToken(parser.nextToken(), parser, XContentParser.Token.START_OBJECT);
                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    expectedToken(parser.currentToken(), parser, XContentParser.Token.FIELD_NAME);

                    expectFieldName(parser, ManageServiceAccountsPrivilege.Fields.MANAGE);
                    privileges.add(ManageServiceAccountsPrivilege.parse(parser));
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

    /**
     * Configurable cluster privileges for ES|QL datasource operations under {@code global.data_source} in role definitions.
     */
    public static class DatasourcePrivileges implements ConfigurableClusterPrivilege {

        public static final TransportVersion ESQL_DATASOURCE_PRIVILEGE = TransportVersion.fromName("esql_datasource_privilege");

        public static final String WRITEABLE_NAME = "datasource-privileges";

        public static final String PRIVILEGE_CREATE = "create";
        public static final String PRIVILEGE_DELETE = "delete";
        public static final String PRIVILEGE_READ_METADATA = "read_metadata";
        public static final String PRIVILEGE_READ = "read";
        public static final String PRIVILEGE_MANAGE = "manage";

        private static final Set<String> ALLOWED_PRIVILEGE_NAMES = Set.of(
            PRIVILEGE_CREATE,
            PRIVILEGE_DELETE,
            PRIVILEGE_READ_METADATA,
            PRIVILEGE_READ,
            PRIVILEGE_MANAGE
        );

        private final List<DatasourcePermissionGroup> groups;
        private final List<CompiledDatasourcePermissionGroup> compiledGroups;
        private final Predicate<TransportRequest> requestPredicate;

        public DatasourcePrivileges(List<DatasourcePermissionGroup> groups) {
            if (groups == null) {
                throw new IllegalArgumentException("datasource privileges require a non-null group list");
            }
            for (DatasourcePermissionGroup group : groups) {
                if (group.names() == null || group.names().length == 0) {
                    throw new IllegalArgumentException("datasource privileges must refer to at least one datasource name or pattern");
                }
                if (group.privileges() == null || group.privileges().length == 0) {
                    throw new IllegalArgumentException("datasource privileges must define at least one privilege");
                }
                for (String raw : group.privileges()) {
                    String privilege = raw.toLowerCase(Locale.ROOT);
                    if (ALLOWED_PRIVILEGE_NAMES.contains(privilege) == false) {
                        throw new IllegalArgumentException("unsupported datasource privilege [" + raw + "]");
                    }
                }
            }
            this.groups = List.copyOf(groups);
            this.compiledGroups = groups.stream().map(CompiledDatasourcePermissionGroup::from).toList();
            this.requestPredicate = buildRequestPredicate();
        }

        private Predicate<TransportRequest> buildRequestPredicate() {
            return request -> {
                if (!(request instanceof DataSourceRequestInfo dsi)) {
                    return false;
                }
                String action = dsi.dataSourceClusterActionName();
                String[] requestNames = dsi.dataSourceNames();
                if (requestNames.length == 0) {
                    return false;
                }
                for (String eachName : requestNames) {
                    boolean covered = compiledGroups.stream().anyMatch(g -> g.matches(eachName, action));
                    if (covered == false) {
                        return false;
                    }
                }
                return true;
            };
        }

        private record CompiledDatasourcePermissionGroup(StringMatcher nameMatcher, Set<String> privilegeNames) {
            private static CompiledDatasourcePermissionGroup from(DatasourcePermissionGroup group) {
                return new CompiledDatasourcePermissionGroup(
                    StringMatcher.of(group.names()),
                    Arrays.stream(group.privileges()).map(s -> s.toLowerCase(Locale.ROOT)).collect(Collectors.toUnmodifiableSet())
                );
            }

            private boolean matches(String datasourceName, String transportAction) {
                return nameMatcher.test(datasourceName) && privilegesAllowAction(privilegeNames, transportAction);
            }
        }

        private static boolean privilegesAllowAction(Set<String> privs, String transportAction) {
            if (privs.contains(PRIVILEGE_MANAGE)) {
                return true;
            }
            if (EsqlDataSourceActionNames.ESQL_PUT_DATA_SOURCE_ACTION_NAME.equals(transportAction)) {
                return privs.contains(PRIVILEGE_CREATE);
            }
            if (EsqlDataSourceActionNames.ESQL_GET_DATA_SOURCE_ACTION_NAME.equals(transportAction)) {
                return privs.contains(PRIVILEGE_READ_METADATA);
            }
            if (EsqlDataSourceActionNames.ESQL_DELETE_DATA_SOURCE_ACTION_NAME.equals(transportAction)) {
                return privs.contains(PRIVILEGE_DELETE);
            }
            if (EsqlDatasetActionNames.ESQL_AUTHORIZE_DATASET_DATASOURCE_ACTION_NAME.equals(transportAction)) {
                return privs.contains(PRIVILEGE_READ);
            }
            return false;
        }

        @Nullable
        public static DatasourcePrivileges parseArray(XContentParser parser) throws IOException {
            List<DatasourcePermissionGroup> groups = new ArrayList<>();
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                expectedToken(token, parser, XContentParser.Token.START_OBJECT);
                String[] names = null;
                String[] privileges = null;
                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    expectedToken(parser.currentToken(), parser, XContentParser.Token.FIELD_NAME);
                    String field = parser.currentName();
                    if (DatasourceFields.NAMES.match(field, parser.getDeprecationHandler())) {
                        expectedToken(parser.nextToken(), parser, XContentParser.Token.START_ARRAY);
                        names = XContentUtils.readStringArray(parser, false);
                    } else if (DatasourceFields.PRIVILEGES.match(field, parser.getDeprecationHandler())) {
                        expectedToken(parser.nextToken(), parser, XContentParser.Token.START_ARRAY);
                        privileges = XContentUtils.readStringArray(parser, false);
                    } else {
                        throw new XContentParseException(parser.getTokenLocation(), "unknown field [" + field + "]");
                    }
                }
                groups.add(new DatasourcePermissionGroup(names, privileges));
            }
            if (groups.isEmpty()) {
                return null;
            }
            return new DatasourcePrivileges(groups);
        }

        /**
         * Writes permission group objects for {@link ConfigurableClusterPrivileges#toXContent}; the caller opens the
         * {@code global.data_source} array.
         */
        public void toXContentArrayElements(XContentBuilder builder, ToXContent.Params params) throws IOException {
            for (DatasourcePermissionGroup g : groups) {
                builder.startObject();
                builder.array(DatasourceFields.NAMES.getPreferredName(), g.names());
                builder.array(DatasourceFields.PRIVILEGES.getPreferredName(), g.privileges());
                builder.endObject();
            }
        }

        @Override
        public Category getCategory() {
            return Category.DATASOURCE;
        }

        @Override
        public String getWriteableName() {
            return WRITEABLE_NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(groups);
        }

        public static DatasourcePrivileges createFrom(StreamInput in) throws IOException {
            return new DatasourcePrivileges(in.readCollectionAsList(DatasourcePermissionGroup::createFrom));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            toXContentArrayElements(builder, params);
            return builder;
        }

        @Override
        public ClusterPermission.Builder buildPermission(ClusterPermission.Builder builder) {
            return builder.add(
                this,
                Set.of(
                    EsqlDataSourceActionNames.ESQL_PUT_DATA_SOURCE_ACTION_NAME,
                    EsqlDataSourceActionNames.ESQL_GET_DATA_SOURCE_ACTION_NAME,
                    EsqlDataSourceActionNames.ESQL_DELETE_DATA_SOURCE_ACTION_NAME,
                    EsqlDatasetActionNames.ESQL_AUTHORIZE_DATASET_DATASOURCE_ACTION_NAME
                ),
                requestPredicate
            );
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DatasourcePrivileges that = (DatasourcePrivileges) o;
            return groups.equals(that.groups);
        }

        @Override
        public int hashCode() {
            return Objects.hash(groups);
        }

        @Override
        public String toString() {
            return "{" + getCategory() + ":[" + Strings.collectionToDelimitedString(groups, ",") + "]}";
        }

        public record DatasourcePermissionGroup(String[] names, String[] privileges) implements Writeable {
            public static DatasourcePermissionGroup createFrom(StreamInput in) throws IOException {
                return new DatasourcePermissionGroup(in.readStringArray(), in.readStringArray());
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeStringArray(names);
                out.writeStringArray(privileges);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                DatasourcePermissionGroup that = (DatasourcePermissionGroup) o;
                return Arrays.equals(names, that.names) && Arrays.equals(privileges, that.privileges);
            }

            @Override
            public int hashCode() {
                return Objects.hash(Arrays.hashCode(names), Arrays.hashCode(privileges));
            }

            @Override
            public String toString() {
                return "DatasourcePermissionGroup[names=" + Arrays.toString(names) + ", privileges=" + Arrays.toString(privileges) + "]";
            }
        }

        private interface DatasourceFields {
            ParseField NAMES = new ParseField("names");
            ParseField PRIVILEGES = new ParseField("privileges");
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
                    Set<IndexPrivilege> privileges = IndexPrivilege.resolveBySelectorAccess(Set.of(indexPatternPrivilege.privileges()));
                    assert privileges.stream().allMatch(p -> p.getSelectorPredicate() != IndexComponentSelectorPredicate.FAILURES)
                        : "not support for failures store access yet";
                    for (IndexPrivilege indexPrivilege : privileges) {
                        indicesPermissionBuilder.addGroup(
                            indexPrivilege,
                            FieldPermissions.DEFAULT,
                            null,
                            false,
                            indexPatternPrivilege.indexPatterns()
                        );
                    }
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
                                        indexPrivilege.getPrivileges(),
                                        indexPrivilege.allowRestrictedIndices()
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
                                                indexPrivilege.getPrivileges(),
                                                indexPrivilege.allowRestrictedIndices()
                                            ) == false
                                        )
                                );
                    } else if (request instanceof final DeleteRoleRequest deleteRoleRequest) {
                        return requestIndexPatternsAllowed(
                            indicesPermission,
                            new String[] { deleteRoleRequest.name() },
                            DELETE_INDEX.name().toArray(String[]::new),
                            false
                        );
                    } else if (request instanceof final BulkDeleteRolesRequest bulkDeleteRoleRequest) {
                        return requestIndexPatternsAllowed(
                            indicesPermission,
                            bulkDeleteRoleRequest.getRoleNames().toArray(String[]::new),
                            DELETE_INDEX.name().toArray(String[]::new),
                            false
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
                    IndexPrivilege namedPrivilege = IndexPrivilege.getNamedOrNull(privilege);

                    // Use resolveBySelectorAccess to determine whether the passed privilege is valid.
                    // IllegalArgumentException is thrown here when an invalid permission is encountered.
                    IndexPrivilege.resolveBySelectorAccess(Set.of(privilege));

                    if (namedPrivilege != null && namedPrivilege.getSelectorPredicate() == IndexComponentSelectorPredicate.FAILURES) {
                        throw new IllegalArgumentException(
                            "Failure store related privileges are not supported as targets of manage roles but found [" + privilege + "]"
                        );
                    }
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
            String[] privileges,
            boolean allowRestrictedIndices
        ) {
            return allowRestrictedIndices == false
                && indicesPermission.checkResourcePrivileges(Set.of(requestIndexPatterns), false, Set.of(privileges), true, null);
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

    /**
     * Delegates the administration of user-managed service accounts and their tokens without handing out
     * {@code manage_security}. The grant names the accounts its holder may write, as wildcard-aware patterns over the
     * {@code namespace/service-name} principal, and the roles they may give those accounts, as patterns of the same kind
     * over role names. An administrator approves the role patterns once; the holder can then write the named accounts
     * freely but never give one a role the patterns do not match. The holder's own privileges are never consulted.
     * <p>
     * A write replaces the account wholesale and authorization sees only the request, so every role the request lists
     * must match the grant, not only the ones being added: a holder cannot keep an unmatched role on an account they write.
     * Each grant is checked on its own, so holding two grants does not allow writing one grant's roles on the other's
     * accounts.
     * A role pattern also covers roles created after the grant, and is matched against names alone, so a pattern such as
     * {@code *} covers every role, reserved roles such as {@code superuser} included.
     * <p>
     * The holder may create tokens for every account the grant names, whatever roles the account holds: roles are
     * checked when an account is written, not when a token is created for it. The names are therefore an ownership
     * boundary. An account that someone else gave roles beyond the grant, an administrator or the holder of another grant
     * with overlapping names, is one whose tokens this holder can create too, so names should cover accounts that belong
     * to the holder alone. The holder may delete the tokens of the same accounts, so that what they create they can also
     * revoke, and list their token names, which deleting a token requires. Listing is shared with built-in accounts under
     * one action, so it refuses the built-in namespace whatever the names cover.
     * <p>
     * The holder may delete the accounts the grant names, whatever roles they hold, by the same ownership reasoning: a
     * write already replaces such an account wholesale.
     * <p>
     * The holder may also get and query the accounts the grant names. Those requests select accounts rather than
     * naming them, so this grants them only once authorization has restricted them to the covered accounts through
     * {@link ServiceAccountReadRequest#restrictToAuthorizedAccounts}.
     */
    public static class ManageServiceAccountsPrivilege implements ConfigurableClusterPrivilege {
        public static final String WRITEABLE_NAME = "manage-service-accounts-privilege";

        public static final TransportVersion SERVICE_ACCOUNT_MANAGE_PRIVILEGE = TransportVersion.fromName(
            "service_account_manage_privilege"
        );

        private final Set<String> names;
        private final Set<String> roles;
        private final Predicate<TransportRequest> requestPredicate;

        public ManageServiceAccountsPrivilege(Set<String> names, Set<String> roles) {
            if (names == null || names.isEmpty()) {
                throw new IllegalArgumentException(
                    "service account manage privileges must refer to at least one service account name or pattern"
                );
            }
            if (roles == null) {
                throw new IllegalArgumentException("service account manage privileges must define the roles that may be assigned");
            }
            this.names = Collections.unmodifiableSet(names);
            this.roles = Collections.unmodifiableSet(roles);
            final Predicate<String> namePredicate = StringMatcher.of(names);
            final Predicate<String> rolePredicate = StringMatcher.of(roles);
            this.requestPredicate = request -> {
                if (request instanceof final PutUserManagedServiceAccountRequest putRequest) {
                    return namePredicate.test(putRequest.getAccountId().asPrincipal())
                        && putRequest.getRoles().stream().allMatch(rolePredicate);
                } else if (request instanceof final CreateServiceAccountTokenRequest tokenRequest) {
                    return namePredicate.test(
                        new ServiceAccountId(tokenRequest.getNamespace(), tokenRequest.getServiceName()).asPrincipal()
                    );
                } else if (request instanceof final DeleteServiceAccountTokenRequest tokenRequest) {
                    return namePredicate.test(
                        new ServiceAccountId(tokenRequest.getNamespace(), tokenRequest.getServiceName()).asPrincipal()
                    );
                } else if (request instanceof final DeleteUserManagedServiceAccountRequest deleteRequest) {
                    return namePredicate.test(deleteRequest.getAccountId().asPrincipal());
                } else if (request instanceof final GetServiceAccountCredentialsRequest credentialsRequest) {
                    return ServiceAccountSettings.BUILTIN_NAMESPACE.equals(credentialsRequest.getNamespace()) == false
                        && namePredicate.test(
                            new ServiceAccountId(credentialsRequest.getNamespace(), credentialsRequest.getServiceName()).asPrincipal()
                        );
                } else if (request instanceof final ServiceAccountReadRequest readRequest) {
                    return readRequest.getAuthorizedAccounts() != null;
                }
                return false;
            };
        }

        @Override
        public Category getCategory() {
            return Category.SERVICE_ACCOUNT;
        }

        public Set<String> getNames() {
            return names;
        }

        public Set<String> getRoles() {
            return roles;
        }

        @Override
        public String getWriteableName() {
            return WRITEABLE_NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringCollection(names);
            out.writeStringCollection(roles);
        }

        public static ManageServiceAccountsPrivilege createFrom(StreamInput in) throws IOException {
            final Set<String> names = in.readCollectionAsSet(StreamInput::readString);
            final Set<String> roles = in.readCollectionAsSet(StreamInput::readString);
            return new ManageServiceAccountsPrivilege(names, roles);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject(Fields.MANAGE.getPreferredName())
                .field(Fields.NAMES.getPreferredName(), names)
                .field(Fields.ROLES.getPreferredName(), roles)
                .endObject();
        }

        public static ManageServiceAccountsPrivilege parse(XContentParser parser) throws IOException {
            expectedToken(parser.currentToken(), parser, XContentParser.Token.FIELD_NAME);
            expectFieldName(parser, Fields.MANAGE);
            expectedToken(parser.nextToken(), parser, XContentParser.Token.START_OBJECT);
            String[] names = null;
            String[] roles = null;
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                expectedToken(parser.currentToken(), parser, XContentParser.Token.FIELD_NAME);
                expectFieldName(parser, Fields.NAMES, Fields.ROLES);
                final boolean isNames = Fields.NAMES.match(parser.currentName(), parser.getDeprecationHandler());
                expectedToken(parser.nextToken(), parser, XContentParser.Token.START_ARRAY);
                final String[] values = XContentUtils.readStringArray(parser, false);
                if (isNames) {
                    names = values;
                } else {
                    roles = values;
                }
            }
            if (names == null || roles == null) {
                throw new XContentParseException(
                    parser.getTokenLocation(),
                    "failed to parse privilege. ["
                        + Fields.MANAGE.getPreferredName()
                        + "] requires both ["
                        + Fields.NAMES.getPreferredName()
                        + "] and ["
                        + Fields.ROLES.getPreferredName()
                        + "]"
                );
            }
            return new ManageServiceAccountsPrivilege(new LinkedHashSet<>(Arrays.asList(names)), new LinkedHashSet<>(Arrays.asList(roles)));
        }

        @Override
        public String toString() {
            return "{"
                + getCategory()
                + ":"
                + Fields.MANAGE.getPreferredName()
                + ":"
                + Fields.NAMES.getPreferredName()
                + "="
                + Strings.collectionToDelimitedString(names, ",")
                + ":"
                + Fields.ROLES.getPreferredName()
                + "="
                + Strings.collectionToDelimitedString(roles, ",")
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
            final ManageServiceAccountsPrivilege that = (ManageServiceAccountsPrivilege) o;
            return names.equals(that.names) && roles.equals(that.roles);
        }

        @Override
        public int hashCode() {
            return Objects.hash(names, roles);
        }

        @Override
        public ClusterPermission.Builder buildPermission(final ClusterPermission.Builder builder) {
            return builder.add(
                this,
                Set.of(
                    PutUserManagedServiceAccountAction.NAME,
                    DeleteUserManagedServiceAccountAction.NAME,
                    CreateUserManagedServiceAccountTokenAction.NAME,
                    DeleteUserManagedServiceAccountTokenAction.NAME,
                    GetServiceAccountCredentialsAction.NAME,
                    GetServiceAccountAction.NAME,
                    QueryServiceAccountAction.NAME
                ),
                requestPredicate
            );
        }

        private interface Fields {
            ParseField MANAGE = new ParseField("manage");
            ParseField NAMES = new ParseField("names");
            ParseField ROLES = new ParseField("roles");
        }
    }
}
