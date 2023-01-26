/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.PrivilegesToCheck;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges;
import org.elasticsearch.xpack.core.security.support.Validation;
import org.elasticsearch.xpack.core.security.xcontent.XContentUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A holder for a Role that contains user-readable information about the Role
 * without containing the actual Role object.
 */
public class RoleDescriptor implements ToXContentObject, Writeable {

    public static final String ROLE_TYPE = "role";
    public static final TransportVersion VERSION_REMOTE_INDICES = TransportVersion.V_8_6_0;

    private final String name;
    private final String[] clusterPrivileges;
    private final ConfigurableClusterPrivilege[] configurableClusterPrivileges;
    private final IndicesPrivileges[] indicesPrivileges;
    private final ApplicationResourcePrivileges[] applicationPrivileges;
    private final String[] runAs;
    private final RemoteIndicesPrivileges[] remoteIndicesPrivileges;
    private final Map<String, Object> metadata;
    private final Map<String, Object> transientMetadata;

    public RoleDescriptor(
        String name,
        @Nullable String[] clusterPrivileges,
        @Nullable IndicesPrivileges[] indicesPrivileges,
        @Nullable String[] runAs
    ) {
        this(name, clusterPrivileges, indicesPrivileges, runAs, null);
    }

    /**
     * @deprecated Use {@link #RoleDescriptor(String, String[], IndicesPrivileges[], ApplicationResourcePrivileges[],
     * ConfigurableClusterPrivilege[], String[], Map, Map, RemoteIndicesPrivileges[])}
     */
    @Deprecated
    public RoleDescriptor(
        String name,
        @Nullable String[] clusterPrivileges,
        @Nullable IndicesPrivileges[] indicesPrivileges,
        @Nullable String[] runAs,
        @Nullable Map<String, Object> metadata
    ) {
        this(name, clusterPrivileges, indicesPrivileges, runAs, metadata, null);
    }

    /**
     * @deprecated Use {@link #RoleDescriptor(String, String[], IndicesPrivileges[], ApplicationResourcePrivileges[],
     * ConfigurableClusterPrivilege[], String[], Map, Map, RemoteIndicesPrivileges[])}
     */
    @Deprecated
    public RoleDescriptor(
        String name,
        @Nullable String[] clusterPrivileges,
        @Nullable IndicesPrivileges[] indicesPrivileges,
        @Nullable String[] runAs,
        @Nullable Map<String, Object> metadata,
        @Nullable Map<String, Object> transientMetadata
    ) {
        this(name, clusterPrivileges, indicesPrivileges, null, null, runAs, metadata, transientMetadata, RemoteIndicesPrivileges.NONE);
    }

    public RoleDescriptor(
        String name,
        @Nullable String[] clusterPrivileges,
        @Nullable IndicesPrivileges[] indicesPrivileges,
        @Nullable ApplicationResourcePrivileges[] applicationPrivileges,
        @Nullable ConfigurableClusterPrivilege[] configurableClusterPrivileges,
        @Nullable String[] runAs,
        @Nullable Map<String, Object> metadata,
        @Nullable Map<String, Object> transientMetadata
    ) {
        this(
            name,
            clusterPrivileges,
            indicesPrivileges,
            applicationPrivileges,
            configurableClusterPrivileges,
            runAs,
            metadata,
            transientMetadata,
            RemoteIndicesPrivileges.NONE
        );
    }

    public RoleDescriptor(
        String name,
        @Nullable String[] clusterPrivileges,
        @Nullable IndicesPrivileges[] indicesPrivileges,
        @Nullable ApplicationResourcePrivileges[] applicationPrivileges,
        @Nullable ConfigurableClusterPrivilege[] configurableClusterPrivileges,
        @Nullable String[] runAs,
        @Nullable Map<String, Object> metadata,
        @Nullable Map<String, Object> transientMetadata,
        @Nullable RemoteIndicesPrivileges[] remoteIndicesPrivileges
    ) {
        this.name = name;
        this.clusterPrivileges = clusterPrivileges != null ? clusterPrivileges : Strings.EMPTY_ARRAY;
        this.configurableClusterPrivileges = sortConfigurableClusterPrivileges(configurableClusterPrivileges);
        this.indicesPrivileges = indicesPrivileges != null ? indicesPrivileges : IndicesPrivileges.NONE;
        this.applicationPrivileges = applicationPrivileges != null ? applicationPrivileges : ApplicationResourcePrivileges.NONE;
        this.runAs = runAs != null ? runAs : Strings.EMPTY_ARRAY;
        this.metadata = metadata != null ? Collections.unmodifiableMap(metadata) : Collections.emptyMap();
        this.transientMetadata = transientMetadata != null
            ? Collections.unmodifiableMap(transientMetadata)
            : Collections.singletonMap("enabled", true);
        this.remoteIndicesPrivileges = remoteIndicesPrivileges != null ? remoteIndicesPrivileges : RemoteIndicesPrivileges.NONE;
    }

    public RoleDescriptor(StreamInput in) throws IOException {
        this.name = in.readString();
        this.clusterPrivileges = in.readStringArray();
        int size = in.readVInt();
        this.indicesPrivileges = new IndicesPrivileges[size];
        for (int i = 0; i < size; i++) {
            indicesPrivileges[i] = new IndicesPrivileges(in);
        }
        this.runAs = in.readStringArray();
        this.metadata = in.readMap();
        this.transientMetadata = in.readMap();

        this.applicationPrivileges = in.readArray(ApplicationResourcePrivileges::new, ApplicationResourcePrivileges[]::new);
        this.configurableClusterPrivileges = ConfigurableClusterPrivileges.readArray(in);
        if (in.getTransportVersion().onOrAfter(VERSION_REMOTE_INDICES)) {
            this.remoteIndicesPrivileges = in.readArray(RemoteIndicesPrivileges::new, RemoteIndicesPrivileges[]::new);
        } else {
            this.remoteIndicesPrivileges = RemoteIndicesPrivileges.NONE;
        }
    }

    public String getName() {
        return this.name;
    }

    public String[] getClusterPrivileges() {
        return this.clusterPrivileges;
    }

    public ConfigurableClusterPrivilege[] getConditionalClusterPrivileges() {
        return this.configurableClusterPrivileges;
    }

    public IndicesPrivileges[] getIndicesPrivileges() {
        return this.indicesPrivileges;
    }

    public RemoteIndicesPrivileges[] getRemoteIndicesPrivileges() {
        return this.remoteIndicesPrivileges;
    }

    public boolean hasRemoteIndicesPrivileges() {
        return remoteIndicesPrivileges.length != 0;
    }

    public ApplicationResourcePrivileges[] getApplicationPrivileges() {
        return this.applicationPrivileges;
    }

    public String[] getRunAs() {
        return this.runAs;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public Map<String, Object> getTransientMetadata() {
        return transientMetadata;
    }

    public boolean isUsingDocumentOrFieldLevelSecurity() {
        return Arrays.stream(indicesPrivileges).anyMatch(ip -> ip.isUsingDocumentLevelSecurity() || ip.isUsingFieldLevelSecurity());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Role[");
        sb.append("name=").append(name);
        sb.append(", cluster=[").append(Strings.arrayToCommaDelimitedString(clusterPrivileges));
        sb.append("], global=[").append(Strings.arrayToCommaDelimitedString(configurableClusterPrivileges));
        sb.append("], indicesPrivileges=[");
        for (IndicesPrivileges group : indicesPrivileges) {
            sb.append(group.toString()).append(",");
        }
        sb.append("], applicationPrivileges=[");
        for (ApplicationResourcePrivileges privilege : applicationPrivileges) {
            sb.append(privilege.toString()).append(",");
        }
        sb.append("], runAs=[").append(Strings.arrayToCommaDelimitedString(runAs));
        sb.append("], metadata=[");
        sb.append(metadata);
        sb.append("]");
        sb.append(", remoteIndicesPrivileges=[");
        for (RemoteIndicesPrivileges group : remoteIndicesPrivileges) {
            sb.append(group.toString()).append(",");
        }
        sb.append("]");
        sb.append("]");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RoleDescriptor that = (RoleDescriptor) o;

        if (name.equals(that.name) == false) return false;
        if (Arrays.equals(clusterPrivileges, that.clusterPrivileges) == false) return false;
        if (Arrays.equals(configurableClusterPrivileges, that.configurableClusterPrivileges) == false) return false;
        if (Arrays.equals(indicesPrivileges, that.indicesPrivileges) == false) return false;
        if (Arrays.equals(applicationPrivileges, that.applicationPrivileges) == false) return false;
        if (metadata.equals(that.getMetadata()) == false) return false;
        if (Arrays.equals(runAs, that.runAs) == false) return false;
        return Arrays.equals(remoteIndicesPrivileges, that.remoteIndicesPrivileges);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + Arrays.hashCode(clusterPrivileges);
        result = 31 * result + Arrays.hashCode(configurableClusterPrivileges);
        result = 31 * result + Arrays.hashCode(indicesPrivileges);
        result = 31 * result + Arrays.hashCode(applicationPrivileges);
        result = 31 * result + Arrays.hashCode(runAs);
        result = 31 * result + metadata.hashCode();
        result = 31 * result + Arrays.hashCode(remoteIndicesPrivileges);
        return result;
    }

    public boolean isEmpty() {
        return clusterPrivileges.length == 0
            && configurableClusterPrivileges.length == 0
            && indicesPrivileges.length == 0
            && applicationPrivileges.length == 0
            && runAs.length == 0
            && metadata.size() == 0
            && remoteIndicesPrivileges.length == 0;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return toXContent(builder, params, false);
    }

    /**
     * Generates x-content for this {@link RoleDescriptor} instance.
     *
     * @param builder     the x-content builder
     * @param params      the parameters for x-content generation directives
     * @param docCreation {@code true} if the x-content is being generated for creating a document
     *                    in the security index, {@code false} if the x-content being generated
     *                    is for API display purposes
     * @return x-content builder
     * @throws IOException if there was an error writing the x-content to the builder
     */
    public XContentBuilder toXContent(XContentBuilder builder, Params params, boolean docCreation) throws IOException {
        builder.startObject();
        builder.array(Fields.CLUSTER.getPreferredName(), clusterPrivileges);
        if (configurableClusterPrivileges.length != 0) {
            builder.field(Fields.GLOBAL.getPreferredName());
            ConfigurableClusterPrivileges.toXContent(builder, params, Arrays.asList(configurableClusterPrivileges));
        }
        builder.xContentList(Fields.INDICES.getPreferredName(), indicesPrivileges);
        builder.xContentList(Fields.APPLICATIONS.getPreferredName(), applicationPrivileges);
        if (runAs != null) {
            builder.array(Fields.RUN_AS.getPreferredName(), runAs);
        }
        builder.field(Fields.METADATA.getPreferredName(), metadata);
        if (docCreation) {
            builder.field(Fields.TYPE.getPreferredName(), ROLE_TYPE);
        } else {
            builder.field(Fields.TRANSIENT_METADATA.getPreferredName(), transientMetadata);
        }
        if (hasRemoteIndicesPrivileges()) {
            builder.xContentList(Fields.REMOTE_INDICES.getPreferredName(), remoteIndicesPrivileges);
        }
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeStringArray(clusterPrivileges);
        out.writeVInt(indicesPrivileges.length);
        for (IndicesPrivileges group : indicesPrivileges) {
            group.writeTo(out);
        }
        out.writeStringArray(runAs);
        out.writeGenericMap(metadata);
        out.writeGenericMap(transientMetadata);
        out.writeArray(ApplicationResourcePrivileges::write, applicationPrivileges);
        ConfigurableClusterPrivileges.writeArray(out, getConditionalClusterPrivileges());
        if (out.getTransportVersion().onOrAfter(VERSION_REMOTE_INDICES)) {
            out.writeArray(remoteIndicesPrivileges);
        } else if (hasRemoteIndicesPrivileges()) {
            throw new IllegalArgumentException(
                "versions of Elasticsearch before ["
                    + VERSION_REMOTE_INDICES
                    + "] can't handle remote indices privileges and attempted to send to ["
                    + out.getTransportVersion()
                    + "]"
            );
        }
    }

    public static RoleDescriptor parse(String name, BytesReference source, boolean allow2xFormat, XContentType xContentType)
        throws IOException {
        assert name != null;
        // EMPTY is safe here because we never use namedObject
        try (
            InputStream stream = source.streamInput();
            XContentParser parser = xContentType.xContent()
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, stream)
        ) {
            return parse(name, parser, allow2xFormat);
        }
    }

    public static RoleDescriptor parse(String name, XContentParser parser, boolean allow2xFormat) throws IOException {
        return parse(name, parser, allow2xFormat, TcpTransport.isUntrustedRemoteClusterEnabled());
    }

    static RoleDescriptor parse(String name, XContentParser parser, boolean allow2xFormat, boolean untrustedRemoteClusterEnabled)
        throws IOException {
        // validate name
        Validation.Error validationError = Validation.Roles.validateRoleName(name, true);
        if (validationError != null) {
            ValidationException ve = new ValidationException();
            ve.addValidationError(validationError.toString());
            throw ve;
        }

        // advance to the START_OBJECT token if needed
        XContentParser.Token token = parser.currentToken() == null ? parser.nextToken() : parser.currentToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("failed to parse role [{}]. expected an object but found [{}] instead", name, token);
        }
        String currentFieldName = null;
        IndicesPrivileges[] indicesPrivileges = null;
        RemoteIndicesPrivileges[] remoteIndicesPrivileges = null;
        String[] clusterPrivileges = null;
        List<ConfigurableClusterPrivilege> configurableClusterPrivileges = Collections.emptyList();
        ApplicationResourcePrivileges[] applicationPrivileges = null;
        String[] runAsUsers = null;
        Map<String, Object> metadata = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (Fields.INDEX.match(currentFieldName, parser.getDeprecationHandler())
                || Fields.INDICES.match(currentFieldName, parser.getDeprecationHandler())) {
                    indicesPrivileges = parseIndices(name, parser, allow2xFormat);
                } else if (Fields.RUN_AS.match(currentFieldName, parser.getDeprecationHandler())) {
                    runAsUsers = readStringArray(name, parser, true);
                } else if (Fields.CLUSTER.match(currentFieldName, parser.getDeprecationHandler())) {
                    clusterPrivileges = readStringArray(name, parser, true);
                } else if (Fields.APPLICATIONS.match(currentFieldName, parser.getDeprecationHandler())
                    || Fields.APPLICATION.match(currentFieldName, parser.getDeprecationHandler())) {
                        applicationPrivileges = parseApplicationPrivileges(name, parser);
                    } else if (Fields.GLOBAL.match(currentFieldName, parser.getDeprecationHandler())) {
                        configurableClusterPrivileges = ConfigurableClusterPrivileges.parse(parser);
                    } else if (Fields.METADATA.match(currentFieldName, parser.getDeprecationHandler())) {
                        if (token != XContentParser.Token.START_OBJECT) {
                            throw new ElasticsearchParseException(
                                "expected field [{}] to be of type object, but found [{}] instead",
                                currentFieldName,
                                token
                            );
                        }
                        metadata = parser.map();
                    } else if (Fields.TRANSIENT_METADATA.match(currentFieldName, parser.getDeprecationHandler())) {
                        if (token == XContentParser.Token.START_OBJECT) {
                            // consume object but just drop
                            parser.map();
                        } else {
                            throw new ElasticsearchParseException(
                                "failed to parse role [{}]. unexpected field [{}]",
                                name,
                                currentFieldName
                            );
                        }
                    } else if (untrustedRemoteClusterEnabled
                        && Fields.REMOTE_INDICES.match(currentFieldName, parser.getDeprecationHandler())) {
                            remoteIndicesPrivileges = parseRemoteIndices(name, parser);
                        } else if (Fields.TYPE.match(currentFieldName, parser.getDeprecationHandler())) {
                            // don't need it
                        } else {
                            throw new ElasticsearchParseException(
                                "failed to parse role [{}]. unexpected field [{}]",
                                name,
                                currentFieldName
                            );
                        }
        }
        return new RoleDescriptor(
            name,
            clusterPrivileges,
            indicesPrivileges,
            applicationPrivileges,
            configurableClusterPrivileges.toArray(new ConfigurableClusterPrivilege[configurableClusterPrivileges.size()]),
            runAsUsers,
            metadata,
            null,
            remoteIndicesPrivileges
        );
    }

    private static String[] readStringArray(String roleName, XContentParser parser, boolean allowNull) throws IOException {
        try {
            return XContentUtils.readStringArray(parser, allowNull);
        } catch (ElasticsearchParseException e) {
            // re-wrap in order to add the role name
            throw new ElasticsearchParseException("failed to parse role [{}]", e, roleName);
        }
    }

    public static PrivilegesToCheck parsePrivilegesToCheck(String description, boolean runDetailedCheck, XContentParser parser)
        throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException(
                "failed to parse privileges check [{}]. expected an object but found [{}] instead",
                description,
                parser.currentToken()
            );
        }
        XContentParser.Token token;
        String currentFieldName = null;
        IndicesPrivileges[] indexPrivileges = null;
        String[] clusterPrivileges = null;
        ApplicationResourcePrivileges[] applicationPrivileges = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (Fields.INDEX.match(currentFieldName, parser.getDeprecationHandler())) {
                indexPrivileges = parseIndices(description, parser, false);
            } else if (Fields.CLUSTER.match(currentFieldName, parser.getDeprecationHandler())) {
                clusterPrivileges = readStringArray(description, parser, true);
            } else if (Fields.APPLICATIONS.match(currentFieldName, parser.getDeprecationHandler())
                || Fields.APPLICATION.match(currentFieldName, parser.getDeprecationHandler())) {
                    applicationPrivileges = parseApplicationPrivileges(description, parser);
                } else {
                    throw new ElasticsearchParseException(
                        "failed to parse privileges check [{}]. unexpected field [{}]",
                        description,
                        currentFieldName
                    );
                }
        }
        if (indexPrivileges == null && clusterPrivileges == null && applicationPrivileges == null) {
            throw new ElasticsearchParseException(
                "failed to parse privileges check [{}]. All privilege fields [{},{},{}] are missing",
                description,
                Fields.CLUSTER,
                Fields.INDEX,
                Fields.APPLICATIONS
            );
        }
        if (indexPrivileges != null) {
            if (Arrays.stream(indexPrivileges).anyMatch(IndicesPrivileges::isUsingFieldLevelSecurity)) {
                throw new ElasticsearchParseException(
                    "Field [{}] is not supported in a has_privileges request",
                    RoleDescriptor.Fields.FIELD_PERMISSIONS
                );
            }
            if (Arrays.stream(indexPrivileges).anyMatch(IndicesPrivileges::isUsingDocumentLevelSecurity)) {
                throw new ElasticsearchParseException("Field [{}] is not supported in a has_privileges request", Fields.QUERY);
            }
        }
        return new PrivilegesToCheck(
            clusterPrivileges != null ? clusterPrivileges : Strings.EMPTY_ARRAY,
            indexPrivileges != null ? indexPrivileges : IndicesPrivileges.NONE,
            applicationPrivileges != null ? applicationPrivileges : ApplicationResourcePrivileges.NONE,
            runDetailedCheck
        );
    }

    /**
     * Parses the privileges to be checked, from the same syntax used for granting privileges in a {@code RoleDescriptor}.
     */
    public static PrivilegesToCheck parsePrivilegesToCheck(
        String description,
        boolean runDetailedCheck,
        BytesReference source,
        XContentType xContentType
    ) throws IOException {
        try (
            InputStream stream = source.streamInput();
            XContentParser parser = xContentType.xContent()
                .createParser(XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE), stream)
        ) {
            // advance to the START_OBJECT token
            XContentParser.Token token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ElasticsearchParseException(
                    "failed to parse privileges check [{}]. expected an object but found [{}] instead",
                    description,
                    token
                );
            }
            return parsePrivilegesToCheck(description, runDetailedCheck, parser);
        }
    }

    private static RoleDescriptor.IndicesPrivileges[] parseIndices(String roleName, XContentParser parser, boolean allow2xFormat)
        throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
            throw new ElasticsearchParseException(
                "failed to parse indices privileges for role [{}]. expected field [{}] value " + "to be an array, but found [{}] instead",
                roleName,
                parser.currentName(),
                parser.currentToken()
            );
        }
        List<RoleDescriptor.IndicesPrivileges> privileges = new ArrayList<>();
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            privileges.add(parseIndex(roleName, parser, allow2xFormat));
        }
        return privileges.toArray(new IndicesPrivileges[privileges.size()]);
    }

    private static IndicesPrivileges parseIndex(final String roleName, final XContentParser parser, final boolean allow2xFormat)
        throws IOException {
        final IndicesPrivilegesWithOptionalRemoteClusters parsed = parseIndexWithOptionalRemoteClusters(
            roleName,
            parser,
            allow2xFormat,
            false
        );
        assert parsed.remoteClusters() == null : "indices privileges cannot have remote clusters";
        return parsed.indicesPrivileges();
    }

    private static RoleDescriptor.RemoteIndicesPrivileges[] parseRemoteIndices(final String roleName, final XContentParser parser)
        throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
            throw new ElasticsearchParseException(
                "failed to parse remote indices privileges for role [{}]. expected field [{}] value "
                    + "to be an array, but found [{}] instead",
                roleName,
                parser.currentName(),
                parser.currentToken()
            );
        }
        final List<RoleDescriptor.RemoteIndicesPrivileges> privileges = new ArrayList<>();
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            privileges.add(parseRemoteIndex(roleName, parser));
        }
        return privileges.toArray(new RemoteIndicesPrivileges[0]);
    }

    private static RemoteIndicesPrivileges parseRemoteIndex(String roleName, XContentParser parser) throws IOException {
        final IndicesPrivilegesWithOptionalRemoteClusters parsed = parseIndexWithOptionalRemoteClusters(roleName, parser, false, true);
        if (parsed.remoteClusters() == null) {
            throw new ElasticsearchParseException(
                "failed to parse remote indices privileges for role [{}]. missing required [{}] field",
                roleName,
                Fields.REMOTE_CLUSTERS
            );
        }
        return new RemoteIndicesPrivileges(parsed.indicesPrivileges(), parsed.remoteClusters());
    }

    private record IndicesPrivilegesWithOptionalRemoteClusters(IndicesPrivileges indicesPrivileges, String[] remoteClusters) {}

    private static IndicesPrivilegesWithOptionalRemoteClusters parseIndexWithOptionalRemoteClusters(
        final String roleName,
        final XContentParser parser,
        final boolean allow2xFormat,
        final boolean allowRemoteClusters
    ) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException(
                "failed to parse indices privileges for role [{}]. expected field [{}] value to "
                    + "be an array of objects, but found an array element of type [{}]",
                roleName,
                parser.currentName(),
                token
            );
        }
        String currentFieldName = null;
        String[] names = null;
        BytesReference query = null;
        String[] privileges = null;
        String[] grantedFields = null;
        String[] deniedFields = null;
        boolean allowRestrictedIndices = false;
        String[] remoteClusters = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (Fields.NAMES.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    names = new String[] { parser.text() };
                } else if (token == XContentParser.Token.START_ARRAY) {
                    names = readStringArray(roleName, parser, false);
                    if (names.length == 0) {
                        throw new ElasticsearchParseException(
                            "failed to parse indices privileges for role [{}]. [{}] cannot be an empty " + "array",
                            roleName,
                            currentFieldName
                        );
                    }
                } else {
                    throw new ElasticsearchParseException(
                        "failed to parse indices privileges for role [{}]. expected field [{}] "
                            + "value to be a string or an array of strings, but found [{}] instead",
                        roleName,
                        currentFieldName,
                        token
                    );
                }
            } else if (Fields.ALLOW_RESTRICTED_INDICES.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token == XContentParser.Token.VALUE_BOOLEAN) {
                    allowRestrictedIndices = parser.booleanValue();
                } else {
                    throw new ElasticsearchParseException(
                        "failed to parse indices privileges for role [{}]. expected field [{}] "
                            + "value to be a boolean, but found [{}] instead",
                        roleName,
                        currentFieldName,
                        token
                    );
                }
            } else if (Fields.QUERY.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token == XContentParser.Token.START_OBJECT) {
                    XContentBuilder builder = JsonXContent.contentBuilder();
                    builder.generator().copyCurrentStructure(parser);
                    query = BytesReference.bytes(builder);
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    final String text = parser.text();
                    if (text.isEmpty() == false) {
                        query = new BytesArray(text);
                    }
                } else if (token != XContentParser.Token.VALUE_NULL) {
                    throw new ElasticsearchParseException(
                        "failed to parse indices privileges for role [{}]. expected field [{}] "
                            + "value to be null, a string, an array, or an object, but found [{}] instead",
                        roleName,
                        currentFieldName,
                        token
                    );
                }
            } else if (Fields.FIELD_PERMISSIONS.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token == XContentParser.Token.START_OBJECT) {
                    token = parser.nextToken();
                    do {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                            if (Fields.GRANT_FIELDS.match(currentFieldName, parser.getDeprecationHandler())) {
                                parser.nextToken();
                                grantedFields = readStringArray(roleName, parser, true);
                                if (grantedFields == null) {
                                    throw new ElasticsearchParseException(
                                        "failed to parse indices privileges for role [{}]. {} must not " + "be null.",
                                        roleName,
                                        Fields.GRANT_FIELDS
                                    );
                                }
                            } else if (Fields.EXCEPT_FIELDS.match(currentFieldName, parser.getDeprecationHandler())) {
                                parser.nextToken();
                                deniedFields = readStringArray(roleName, parser, true);
                                if (deniedFields == null) {
                                    throw new ElasticsearchParseException(
                                        "failed to parse indices privileges for role [{}]. {} must not " + "be null.",
                                        roleName,
                                        Fields.EXCEPT_FIELDS
                                    );
                                }
                            } else {
                                throw new ElasticsearchParseException(
                                    "failed to parse indices privileges for role [{}]. "
                                        + "\"{}\" only accepts options {} and {}, but got: {}",
                                    roleName,
                                    Fields.FIELD_PERMISSIONS,
                                    Fields.GRANT_FIELDS,
                                    Fields.EXCEPT_FIELDS,
                                    parser.currentName()
                                );
                            }
                        } else {
                            if (token == XContentParser.Token.END_OBJECT) {
                                throw new ElasticsearchParseException(
                                    "failed to parse indices privileges for role [{}]. " + "\"{}\" must not be empty.",
                                    roleName,
                                    Fields.FIELD_PERMISSIONS
                                );
                            } else {
                                throw new ElasticsearchParseException(
                                    "failed to parse indices privileges for role [{}]. expected {} but " + "got {}.",
                                    roleName,
                                    XContentParser.Token.FIELD_NAME,
                                    token
                                );
                            }
                        }
                    } while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT);
                } else {
                    throw new ElasticsearchParseException(
                        "failed to parse indices privileges for role [{}]. expected {} but got {} in \"{}\".",
                        roleName,
                        XContentParser.Token.START_OBJECT,
                        token,
                        Fields.FIELD_PERMISSIONS
                    );
                }
            } else if (Fields.PRIVILEGES.match(currentFieldName, parser.getDeprecationHandler())) {
                privileges = readStringArray(roleName, parser, true);
            } else if (Fields.FIELD_PERMISSIONS_2X.match(currentFieldName, parser.getDeprecationHandler())) {
                if (allow2xFormat) {
                    grantedFields = readStringArray(roleName, parser, true);
                } else {
                    throw new ElasticsearchParseException(
                        """
                            ["fields": [...]] format has changed for field permissions in role [{}], \
                            use ["{}": {"{}":[...],"{}":[...]}] instead""",
                        roleName,
                        Fields.FIELD_PERMISSIONS,
                        Fields.GRANT_FIELDS,
                        Fields.EXCEPT_FIELDS,
                        roleName
                    );
                }
            } else if (Fields.TRANSIENT_METADATA.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token == XContentParser.Token.START_OBJECT) {
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        // it is transient metadata, skip it
                    }
                } else {
                    throw new ElasticsearchParseException(
                        "failed to parse transient metadata for role [{}]. expected {} but got {}" + " in \"{}\".",
                        roleName,
                        XContentParser.Token.START_OBJECT,
                        token,
                        Fields.TRANSIENT_METADATA
                    );
                }
            } else if (allowRemoteClusters && Fields.REMOTE_CLUSTERS.match(currentFieldName, parser.getDeprecationHandler())) {
                remoteClusters = readStringArray(roleName, parser, false);
            } else {
                throw new ElasticsearchParseException(
                    "failed to parse indices privileges for role [{}]. unexpected field [{}]",
                    roleName,
                    currentFieldName
                );
            }
        }
        if (names == null) {
            throw new ElasticsearchParseException(
                "failed to parse indices privileges for role [{}]. missing required [{}] field",
                roleName,
                Fields.NAMES.getPreferredName()
            );
        }
        if (privileges == null) {
            throw new ElasticsearchParseException(
                "failed to parse indices privileges for role [{}]. missing required [{}] field",
                roleName,
                Fields.PRIVILEGES.getPreferredName()
            );
        }
        if (deniedFields != null && grantedFields == null) {
            throw new ElasticsearchParseException(
                "failed to parse indices privileges for role [{}]. {} requires {} if {} is given",
                roleName,
                Fields.FIELD_PERMISSIONS,
                Fields.GRANT_FIELDS,
                Fields.EXCEPT_FIELDS
            );
        }
        checkIfExceptFieldsIsSubsetOfGrantedFields(roleName, grantedFields, deniedFields);
        return new IndicesPrivilegesWithOptionalRemoteClusters(
            IndicesPrivileges.builder()
                .indices(names)
                .privileges(privileges)
                .grantedFields(grantedFields)
                .deniedFields(deniedFields)
                .query(query)
                .allowRestrictedIndices(allowRestrictedIndices)
                .build(),
            remoteClusters
        );
    }

    private static ConfigurableClusterPrivilege[] sortConfigurableClusterPrivileges(
        ConfigurableClusterPrivilege[] configurableClusterPrivileges
    ) {
        if (null == configurableClusterPrivileges) {
            return ConfigurableClusterPrivileges.EMPTY_ARRAY;
        } else if (configurableClusterPrivileges.length < 2) {
            return configurableClusterPrivileges;
        } else {
            ConfigurableClusterPrivilege[] configurableClusterPrivilegesCopy = Arrays.copyOf(
                configurableClusterPrivileges,
                configurableClusterPrivileges.length
            );
            Arrays.sort(configurableClusterPrivilegesCopy, Comparator.comparingInt(o -> o.getCategory().ordinal()));
            return configurableClusterPrivilegesCopy;
        }
    }

    private static void checkIfExceptFieldsIsSubsetOfGrantedFields(String roleName, String[] grantedFields, String[] deniedFields) {
        try {
            FieldPermissions.buildPermittedFieldsAutomaton(grantedFields, deniedFields);
        } catch (ElasticsearchSecurityException e) {
            throw new ElasticsearchParseException("failed to parse indices privileges for role [{}] - {}", e, roleName, e.getMessage());
        }
    }

    private static ApplicationResourcePrivileges[] parseApplicationPrivileges(String roleName, XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
            throw new ElasticsearchParseException(
                "failed to parse application privileges for role [{}]. expected field [{}] value "
                    + "to be an array, but found [{}] instead",
                roleName,
                parser.currentName(),
                parser.currentToken()
            );
        }
        List<ApplicationResourcePrivileges> privileges = new ArrayList<>();
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            privileges.add(parseApplicationPrivilege(roleName, parser));
        }
        return privileges.toArray(new ApplicationResourcePrivileges[privileges.size()]);
    }

    private static ApplicationResourcePrivileges parseApplicationPrivilege(String roleName, XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException(
                "failed to parse application privileges for role [{}]. expected field [{}] value to "
                    + "be an array of objects, but found an array element of type [{}]",
                roleName,
                parser.currentName(),
                token
            );
        }
        final ApplicationResourcePrivileges.Builder builder = ApplicationResourcePrivileges.PARSER.parse(parser, null);
        if (builder.hasResources() == false) {
            throw new ElasticsearchParseException(
                "failed to parse application privileges for role [{}]. missing required [{}] field",
                roleName,
                Fields.RESOURCES.getPreferredName()
            );
        }
        if (builder.hasPrivileges() == false) {
            throw new ElasticsearchParseException(
                "failed to parse application privileges for role [{}]. missing required [{}] field",
                roleName,
                Fields.PRIVILEGES.getPreferredName()
            );
        }
        return builder.build();
    }

    public static final class RemoteIndicesPrivileges implements Writeable, ToXContentObject {

        private static final RemoteIndicesPrivileges[] NONE = new RemoteIndicesPrivileges[0];

        private final IndicesPrivileges indicesPrivileges;
        private final String[] remoteClusters;

        public RemoteIndicesPrivileges(IndicesPrivileges indicesPrivileges, String... remoteClusters) {
            this.indicesPrivileges = indicesPrivileges;
            this.remoteClusters = remoteClusters;
        }

        public RemoteIndicesPrivileges(StreamInput in) throws IOException {
            this(new IndicesPrivileges(in), in.readStringArray());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            indicesPrivileges.innerToXContent(builder);
            builder.array(Fields.REMOTE_CLUSTERS.getPreferredName(), remoteClusters);
            return builder.endObject();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            indicesPrivileges.writeTo(out);
            out.writeStringArray(remoteClusters);
        }

        public static Builder builder(String... remoteClusters) {
            return new Builder(remoteClusters);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            RemoteIndicesPrivileges that = (RemoteIndicesPrivileges) o;

            if (false == indicesPrivileges.equals(that.indicesPrivileges)) return false;
            return Arrays.equals(remoteClusters, that.remoteClusters);
        }

        @Override
        public int hashCode() {
            int result = indicesPrivileges.hashCode();
            result = 31 * result + Arrays.hashCode(remoteClusters);
            return result;
        }

        @Override
        public String toString() {
            return "RemoteIndicesPrivileges{"
                + "indicesPrivileges="
                + indicesPrivileges
                + ", remoteClusters="
                + Strings.arrayToCommaDelimitedString(remoteClusters)
                + '}';
        }

        public IndicesPrivileges indicesPrivileges() {
            return indicesPrivileges;
        }

        public String[] remoteClusters() {
            return remoteClusters;
        }

        public static class Builder {
            private final IndicesPrivileges.Builder indicesBuilder = new IndicesPrivileges.Builder();
            private final String[] remoteClusters;

            public Builder(String... remoteClusters) {
                this.remoteClusters = remoteClusters;
            }

            public Builder indices(String... indices) {
                indicesBuilder.indices(indices);
                return this;
            }

            public Builder privileges(String... privileges) {
                indicesBuilder.privileges(privileges);
                return this;
            }

            public Builder privileges(Collection<String> privileges) {
                return privileges(privileges.toArray(new String[0]));
            }

            public Builder grantedFields(String... grantedFields) {
                indicesBuilder.grantedFields(grantedFields);
                return this;
            }

            public Builder deniedFields(String... deniedFields) {
                indicesBuilder.deniedFields(deniedFields);
                return this;
            }

            public Builder query(@Nullable String query) {
                return query(query == null ? null : new BytesArray(query));
            }

            public Builder query(@Nullable BytesReference query) {
                indicesBuilder.query(query);
                return this;
            }

            public Builder allowRestrictedIndices(boolean allow) {
                indicesBuilder.allowRestrictedIndices(allow);
                return this;
            }

            public RemoteIndicesPrivileges build() {
                if (remoteClusters == null || remoteClusters.length == 0) {
                    throw new IllegalArgumentException(
                        "the ["
                            + Fields.REMOTE_INDICES
                            + "] sub-field ["
                            + Fields.REMOTE_CLUSTERS
                            + "] must refer to at least one cluster alias or cluster alias pattern"
                    );
                }
                return new RemoteIndicesPrivileges(indicesBuilder.build(), remoteClusters);
            }
        }
    }

    /**
     * A class representing permissions for a group of indices mapped to
     * privileges, field permissions, and a query.
     */
    public static class IndicesPrivileges implements ToXContentObject, Writeable, Comparable<IndicesPrivileges> {

        private static final IndicesPrivileges[] NONE = new IndicesPrivileges[0];

        private String[] indices;
        private String[] privileges;
        private String[] grantedFields = null;
        private String[] deniedFields = null;
        private BytesReference query;
        // by default certain restricted indices are exempted when granting privileges, as they should generally be hidden for ordinary
        // users. Setting this flag eliminates this special status, and any index name pattern in the permission will cover restricted
        // indices as well.
        private boolean allowRestrictedIndices = false;

        private IndicesPrivileges() {}

        public IndicesPrivileges(StreamInput in) throws IOException {
            this.indices = in.readStringArray();
            this.grantedFields = in.readOptionalStringArray();
            this.deniedFields = in.readOptionalStringArray();
            this.privileges = in.readStringArray();
            this.query = in.readOptionalBytesReference();
            this.allowRestrictedIndices = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringArray(indices);
            out.writeOptionalStringArray(grantedFields);
            out.writeOptionalStringArray(deniedFields);
            out.writeStringArray(privileges);
            out.writeOptionalBytesReference(query);
            out.writeBoolean(allowRestrictedIndices);
        }

        public static Builder builder() {
            return new Builder();
        }

        public String[] getIndices() {
            return this.indices;
        }

        public String[] getPrivileges() {
            return this.privileges;
        }

        @Nullable
        public String[] getGrantedFields() {
            return this.grantedFields;
        }

        @Nullable
        public String[] getDeniedFields() {
            return this.deniedFields;
        }

        @Nullable
        public BytesReference getQuery() {
            return this.query;
        }

        public boolean isUsingDocumentLevelSecurity() {
            return query != null;
        }

        public boolean isUsingFieldLevelSecurity() {
            return hasDeniedFields() || hasGrantedFields();
        }

        public boolean allowRestrictedIndices() {
            return allowRestrictedIndices;
        }

        public boolean hasDeniedFields() {
            return deniedFields != null && deniedFields.length > 0;
        }

        public boolean hasGrantedFields() {
            if (grantedFields != null && grantedFields.length >= 0) {
                // we treat just '*' as no FLS since that's what the UI defaults to
                if (grantedFields.length == 1 && "*".equals(grantedFields[0])) {
                    return false;
                } else {
                    return true;
                }
            }
            return false;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("IndicesPrivileges[");
            sb.append("indices=[").append(Strings.arrayToCommaDelimitedString(indices));
            sb.append("], allowRestrictedIndices=[").append(allowRestrictedIndices);
            sb.append("], privileges=[").append(Strings.arrayToCommaDelimitedString(privileges));
            sb.append("], ");
            if (grantedFields != null || deniedFields != null) {
                sb.append(RoleDescriptor.Fields.FIELD_PERMISSIONS).append("=[");
                if (grantedFields == null) {
                    sb.append(RoleDescriptor.Fields.GRANT_FIELDS).append("=null");
                } else {
                    sb.append(RoleDescriptor.Fields.GRANT_FIELDS).append("=[").append(Strings.arrayToCommaDelimitedString(grantedFields));
                    sb.append("]");
                }
                if (deniedFields == null) {
                    sb.append(", ").append(RoleDescriptor.Fields.EXCEPT_FIELDS).append("=null");
                } else {
                    sb.append(", ")
                        .append(RoleDescriptor.Fields.EXCEPT_FIELDS)
                        .append("=[")
                        .append(Strings.arrayToCommaDelimitedString(deniedFields));
                    sb.append("]");
                }
                sb.append("]");
            }
            if (query != null) {
                sb.append(", query=");
                sb.append(query.utf8ToString());
            }
            sb.append("]");
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            IndicesPrivileges that = (IndicesPrivileges) o;

            if (Arrays.equals(indices, that.indices) == false) return false;
            if (allowRestrictedIndices != that.allowRestrictedIndices) return false;
            if (Arrays.equals(privileges, that.privileges) == false) return false;
            if (Arrays.equals(grantedFields, that.grantedFields) == false) return false;
            if (Arrays.equals(deniedFields, that.deniedFields) == false) return false;
            return Objects.equals(query, that.query);
        }

        @Override
        public int hashCode() {
            int result = Arrays.hashCode(indices);
            result = 31 * result + (allowRestrictedIndices ? 1 : 0);
            result = 31 * result + Arrays.hashCode(privileges);
            result = 31 * result + Arrays.hashCode(grantedFields);
            result = 31 * result + Arrays.hashCode(deniedFields);
            result = 31 * result + (query != null ? query.hashCode() : 0);
            return result;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            innerToXContent(builder);
            return builder.endObject();
        }

        XContentBuilder innerToXContent(XContentBuilder builder) throws IOException {
            builder.array("names", indices);
            builder.array("privileges", privileges);
            if (grantedFields != null || deniedFields != null) {
                builder.startObject(RoleDescriptor.Fields.FIELD_PERMISSIONS.getPreferredName());
                if (grantedFields != null) {
                    builder.array(RoleDescriptor.Fields.GRANT_FIELDS.getPreferredName(), grantedFields);
                }
                if (deniedFields != null) {
                    builder.array(RoleDescriptor.Fields.EXCEPT_FIELDS.getPreferredName(), deniedFields);
                }
                builder.endObject();
            }
            if (query != null) {
                builder.field("query", query.utf8ToString());
            }
            return builder.field(RoleDescriptor.Fields.ALLOW_RESTRICTED_INDICES.getPreferredName(), allowRestrictedIndices);
        }

        public static void write(StreamOutput out, IndicesPrivileges privileges) throws IOException {
            privileges.writeTo(out);
        }

        @Override
        public int compareTo(IndicesPrivileges o) {
            if (this == o) {
                return 0;
            }
            int cmp = Boolean.compare(allowRestrictedIndices, o.allowRestrictedIndices);
            if (cmp != 0) {
                return cmp;
            }
            cmp = Arrays.compare(indices, o.indices);
            if (cmp != 0) {
                return cmp;
            }
            cmp = Arrays.compare(privileges, o.privileges);
            if (cmp != 0) {
                return cmp;
            }
            cmp = Objects.compare(query, o.query, Comparator.nullsFirst(BytesReference::compareTo));
            if (cmp != 0) {
                return cmp;
            }
            cmp = Arrays.compare(grantedFields, o.grantedFields);
            if (cmp != 0) {
                return cmp;
            }
            cmp = Arrays.compare(deniedFields, o.deniedFields);
            return cmp;
        }

        public static class Builder {

            private IndicesPrivileges indicesPrivileges = new IndicesPrivileges();

            private Builder() {}

            public Builder indices(String... indices) {
                indicesPrivileges.indices = indices;
                return this;
            }

            public Builder indices(Collection<String> indices) {
                return indices(indices.toArray(new String[indices.size()]));
            }

            public Builder privileges(String... privileges) {
                indicesPrivileges.privileges = privileges;
                return this;
            }

            public Builder privileges(Collection<String> privileges) {
                return privileges(privileges.toArray(new String[privileges.size()]));
            }

            public Builder grantedFields(String... grantedFields) {
                indicesPrivileges.grantedFields = grantedFields;
                return this;
            }

            public Builder deniedFields(String... deniedFields) {
                indicesPrivileges.deniedFields = deniedFields;
                return this;
            }

            public Builder query(@Nullable String query) {
                return query(query == null ? null : new BytesArray(query));
            }

            public Builder allowRestrictedIndices(boolean allow) {
                indicesPrivileges.allowRestrictedIndices = allow;
                return this;
            }

            public Builder query(@Nullable BytesReference query) {
                if (query == null) {
                    indicesPrivileges.query = null;
                } else {
                    indicesPrivileges.query = query;
                }
                return this;
            }

            public IndicesPrivileges build() {
                if (indicesPrivileges.indices == null || indicesPrivileges.indices.length == 0) {
                    throw new IllegalArgumentException("indices privileges must refer to at least one index name or index name pattern");
                }
                if (indicesPrivileges.privileges == null || indicesPrivileges.privileges.length == 0) {
                    throw new IllegalArgumentException("indices privileges must define at least one privilege");
                }
                return indicesPrivileges;
            }
        }
    }

    public static class ApplicationResourcePrivileges implements ToXContentObject, Writeable {

        private static final ApplicationResourcePrivileges[] NONE = new ApplicationResourcePrivileges[0];
        private static final ObjectParser<ApplicationResourcePrivileges.Builder, Void> PARSER = new ObjectParser<>(
            "application",
            ApplicationResourcePrivileges::builder
        );

        static {
            PARSER.declareString(Builder::application, Fields.APPLICATION);
            PARSER.declareStringArray(Builder::privileges, Fields.PRIVILEGES);
            PARSER.declareStringArray(Builder::resources, Fields.RESOURCES);
        }

        private String application;
        private String[] privileges;
        private String[] resources;

        private ApplicationResourcePrivileges() {}

        public ApplicationResourcePrivileges(StreamInput in) throws IOException {
            this.application = in.readString();
            this.privileges = in.readStringArray();
            this.resources = in.readStringArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(application);
            out.writeStringArray(privileges);
            out.writeStringArray(resources);
        }

        public static Builder builder() {
            return new Builder();
        }

        public String getApplication() {
            return application;
        }

        public String[] getResources() {
            return this.resources;
        }

        public String[] getPrivileges() {
            return this.privileges;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(getClass().getSimpleName()).append("[application=")
                .append(application)
                .append(", privileges=[")
                .append(Strings.arrayToCommaDelimitedString(privileges))
                .append("], resources=[")
                .append(Strings.arrayToCommaDelimitedString(resources))
                .append("]]");
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || this.getClass() != o.getClass()) {
                return false;
            }

            ApplicationResourcePrivileges that = (ApplicationResourcePrivileges) o;

            return Objects.equals(this.application, that.application)
                && Arrays.equals(this.resources, that.resources)
                && Arrays.equals(this.privileges, that.privileges);
        }

        @Override
        public int hashCode() {
            int result = Arrays.hashCode(resources);
            result = 31 * result + Arrays.hashCode(privileges);
            return result;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Fields.APPLICATION.getPreferredName(), application);
            builder.array(Fields.PRIVILEGES.getPreferredName(), privileges);
            builder.array(Fields.RESOURCES.getPreferredName(), resources);
            return builder.endObject();
        }

        public static void write(StreamOutput out, ApplicationResourcePrivileges privileges) throws IOException {
            privileges.writeTo(out);
        }

        public static class Builder {

            private ApplicationResourcePrivileges applicationPrivileges = new ApplicationResourcePrivileges();

            private Builder() {}

            public Builder application(String appName) {
                applicationPrivileges.application = appName;
                return this;
            }

            public Builder resources(String... resources) {
                applicationPrivileges.resources = resources;
                return this;
            }

            public Builder resources(Collection<String> resources) {
                return resources(resources.toArray(new String[resources.size()]));
            }

            public Builder privileges(String... privileges) {
                applicationPrivileges.privileges = privileges;
                return this;
            }

            public Builder privileges(Collection<String> privileges) {
                return privileges(privileges.toArray(new String[privileges.size()]));
            }

            public boolean hasResources() {
                return applicationPrivileges.resources != null;
            }

            public boolean hasPrivileges() {
                return applicationPrivileges.privileges != null;
            }

            public ApplicationResourcePrivileges build() {
                if (Strings.isNullOrEmpty(applicationPrivileges.application)) {
                    throw new IllegalArgumentException("application privileges must have an application name");
                }
                if (applicationPrivileges.privileges == null || applicationPrivileges.privileges.length == 0) {
                    throw new IllegalArgumentException("application privileges must define at least one privilege");
                }
                if (applicationPrivileges.resources == null || applicationPrivileges.resources.length == 0) {
                    throw new IllegalArgumentException("application privileges must refer to at least one resource");
                }
                return applicationPrivileges;
            }

        }
    }

    public interface Fields {
        ParseField CLUSTER = new ParseField("cluster");
        ParseField GLOBAL = new ParseField("global");
        ParseField INDEX = new ParseField("index");
        ParseField INDICES = new ParseField("indices");
        ParseField REMOTE_INDICES = new ParseField("remote_indices");
        ParseField APPLICATIONS = new ParseField("applications");
        ParseField RUN_AS = new ParseField("run_as");
        ParseField NAMES = new ParseField("names");
        ParseField ALLOW_RESTRICTED_INDICES = new ParseField("allow_restricted_indices");
        ParseField RESOURCES = new ParseField("resources");
        ParseField QUERY = new ParseField("query");
        ParseField PRIVILEGES = new ParseField("privileges");
        ParseField REMOTE_CLUSTERS = new ParseField("clusters");
        ParseField APPLICATION = new ParseField("application");
        ParseField FIELD_PERMISSIONS = new ParseField("field_security");
        ParseField FIELD_PERMISSIONS_2X = new ParseField("fields");
        ParseField GRANT_FIELDS = new ParseField("grant");
        ParseField EXCEPT_FIELDS = new ParseField("except");
        ParseField METADATA = new ParseField("metadata");
        ParseField TRANSIENT_METADATA = new ParseField("transient_metadata");
        ParseField TYPE = new ParseField("type");
    }
}
