/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authz;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A holder for a Role that contains user-readable information about the Role
 * without containing the actual Role object.
 */
public class RoleDescriptor implements ToXContentObject, Writeable {

    public static final String ROLE_TYPE = "role";

    private final String name;
    private final String[] clusterPrivileges;
    private final ConfigurableClusterPrivilege[] configurableClusterPrivileges;
    private final IndicesPrivileges[] indicesPrivileges;
    private final ApplicationResourcePrivileges[] applicationPrivileges;
    private final String[] runAs;
    private final Map<String, Object> metadata;
    private final Map<String, Object> transientMetadata;

    public RoleDescriptor(String name,
                          @Nullable String[] clusterPrivileges,
                          @Nullable IndicesPrivileges[] indicesPrivileges,
                          @Nullable String[] runAs) {
        this(name, clusterPrivileges, indicesPrivileges, runAs, null);
    }

    /**
     * @deprecated Use {@link #RoleDescriptor(String, String[], IndicesPrivileges[], ApplicationResourcePrivileges[],
     * ConfigurableClusterPrivilege[], String[], Map, Map)}
     */
    @Deprecated
    public RoleDescriptor(String name,
                          @Nullable String[] clusterPrivileges,
                          @Nullable IndicesPrivileges[] indicesPrivileges,
                          @Nullable String[] runAs,
                          @Nullable Map<String, Object> metadata) {
        this(name, clusterPrivileges, indicesPrivileges, runAs, metadata, null);
    }

    /**
     * @deprecated Use {@link #RoleDescriptor(String, String[], IndicesPrivileges[], ApplicationResourcePrivileges[],
     * ConfigurableClusterPrivilege[], String[], Map, Map)}
     */
    @Deprecated
    public RoleDescriptor(String name,
                          @Nullable String[] clusterPrivileges,
                          @Nullable IndicesPrivileges[] indicesPrivileges,
                          @Nullable String[] runAs,
                          @Nullable Map<String, Object> metadata,
                          @Nullable Map<String, Object> transientMetadata) {
        this(name, clusterPrivileges, indicesPrivileges, null, null, runAs, metadata, transientMetadata);
    }

    public RoleDescriptor(String name,
                          @Nullable String[] clusterPrivileges,
                          @Nullable IndicesPrivileges[] indicesPrivileges,
                          @Nullable ApplicationResourcePrivileges[] applicationPrivileges,
                          @Nullable ConfigurableClusterPrivilege[] configurableClusterPrivileges,
                          @Nullable String[] runAs,
                          @Nullable Map<String, Object> metadata,
                          @Nullable Map<String, Object> transientMetadata) {
        this.name = name;
        this.clusterPrivileges = clusterPrivileges != null ? clusterPrivileges : Strings.EMPTY_ARRAY;
        this.configurableClusterPrivileges = configurableClusterPrivileges != null
            ? configurableClusterPrivileges : ConfigurableClusterPrivileges.EMPTY_ARRAY;
        this.indicesPrivileges = indicesPrivileges != null ? indicesPrivileges : IndicesPrivileges.NONE;
        this.applicationPrivileges = applicationPrivileges != null ? applicationPrivileges : ApplicationResourcePrivileges.NONE;
        this.runAs = runAs != null ? runAs : Strings.EMPTY_ARRAY;
        this.metadata = metadata != null ? Collections.unmodifiableMap(metadata) : Collections.emptyMap();
        this.transientMetadata = transientMetadata != null ? Collections.unmodifiableMap(transientMetadata) :
                Collections.singletonMap("enabled", true);
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
        sb.append("]]");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RoleDescriptor that = (RoleDescriptor) o;

        if (!name.equals(that.name)) return false;
        if (!Arrays.equals(clusterPrivileges, that.clusterPrivileges)) return false;
        if (!Arrays.equals(configurableClusterPrivileges, that.configurableClusterPrivileges)) return false;
        if (!Arrays.equals(indicesPrivileges, that.indicesPrivileges)) return false;
        if (!Arrays.equals(applicationPrivileges, that.applicationPrivileges)) return false;
        if (!metadata.equals(that.getMetadata())) return false;
        return Arrays.equals(runAs, that.runAs);
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
        return result;
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
        builder.array(Fields.INDICES.getPreferredName(), (Object[]) indicesPrivileges);
        builder.array(Fields.APPLICATIONS.getPreferredName(), (Object[]) applicationPrivileges);
        if (runAs != null) {
            builder.array(Fields.RUN_AS.getPreferredName(), runAs);
        }
        builder.field(Fields.METADATA.getPreferredName(), metadata);
        if (docCreation) {
            builder.field(Fields.TYPE.getPreferredName(), ROLE_TYPE);
        } else {
            builder.field(Fields.TRANSIENT_METADATA.getPreferredName(), transientMetadata);
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
        out.writeMap(metadata);
        out.writeMap(transientMetadata);
        out.writeArray(ApplicationResourcePrivileges::write, applicationPrivileges);
        ConfigurableClusterPrivileges.writeArray(out, getConditionalClusterPrivileges());
    }

    public static RoleDescriptor parse(String name, BytesReference source, boolean allow2xFormat, XContentType xContentType)
            throws IOException {
        assert name != null;
        // EMPTY is safe here because we never use namedObject
        try (InputStream stream = source.streamInput();
             XContentParser parser = xContentType.xContent()
                     .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, stream)) {
            return parse(name, parser, allow2xFormat);
        }
    }

    public static RoleDescriptor parse(String name, XContentParser parser, boolean allow2xFormat) throws IOException {
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
                            "expected field [{}] to be of type object, but found [{}] instead", currentFieldName, token);
                }
                metadata = parser.map();
            } else if (Fields.TRANSIENT_METADATA.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token == XContentParser.Token.START_OBJECT) {
                    // consume object but just drop
                    parser.map();
                } else {
                    throw new ElasticsearchParseException("failed to parse role [{}]. unexpected field [{}]", name, currentFieldName);
                }
            } else if (Fields.TYPE.match(currentFieldName, parser.getDeprecationHandler())) {
                // don't need it
            } else {
                throw new ElasticsearchParseException("failed to parse role [{}]. unexpected field [{}]", name, currentFieldName);
            }
        }
        return new RoleDescriptor(name, clusterPrivileges, indicesPrivileges, applicationPrivileges,
            configurableClusterPrivileges.toArray(new ConfigurableClusterPrivilege[configurableClusterPrivileges.size()]), runAsUsers,
            metadata, null);
    }

    private static String[] readStringArray(String roleName, XContentParser parser, boolean allowNull) throws IOException {
        try {
            return XContentUtils.readStringArray(parser, allowNull);
        } catch (ElasticsearchParseException e) {
            // re-wrap in order to add the role name
            throw new ElasticsearchParseException("failed to parse role [{}]", e, roleName);
        }
    }

    public static RoleDescriptor parsePrivilegesCheck(String description, BytesReference source, XContentType xContentType)
            throws IOException {
        try (InputStream stream = source.streamInput();
             XContentParser parser = xContentType.xContent()
                     .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, stream)) {
            // advance to the START_OBJECT token
            XContentParser.Token token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ElasticsearchParseException("failed to parse privileges check [{}]. expected an object but found [{}] instead",
                        description, token);
            }
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
                    throw new ElasticsearchParseException("failed to parse privileges check [{}]. unexpected field [{}]",
                            description, currentFieldName);
                }
            }
            if (indexPrivileges == null && clusterPrivileges == null && applicationPrivileges == null) {
                throw new ElasticsearchParseException("failed to parse privileges check [{}]. All privilege fields [{},{},{}] are missing",
                        description, Fields.CLUSTER, Fields.INDEX, Fields.APPLICATIONS);
            }
            if (indexPrivileges != null) {
                if (Arrays.stream(indexPrivileges).anyMatch(IndicesPrivileges::isUsingFieldLevelSecurity)) {
                    throw new ElasticsearchParseException("Field [{}] is not supported in a has_privileges request",
                            RoleDescriptor.Fields.FIELD_PERMISSIONS);
                }
                if (Arrays.stream(indexPrivileges).anyMatch(IndicesPrivileges::isUsingDocumentLevelSecurity)) {
                    throw new ElasticsearchParseException("Field [{}] is not supported in a has_privileges request", Fields.QUERY);
                }
            }
            return new RoleDescriptor(description, clusterPrivileges, indexPrivileges, applicationPrivileges, null, null, null, null);
        }
    }

    private static RoleDescriptor.IndicesPrivileges[] parseIndices(String roleName, XContentParser parser,
                                                                   boolean allow2xFormat) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
            throw new ElasticsearchParseException("failed to parse indices privileges for role [{}]. expected field [{}] value " +
                    "to be an array, but found [{}] instead", roleName, parser.currentName(), parser.currentToken());
        }
        List<RoleDescriptor.IndicesPrivileges> privileges = new ArrayList<>();
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            privileges.add(parseIndex(roleName, parser, allow2xFormat));
        }
        return privileges.toArray(new IndicesPrivileges[privileges.size()]);
    }

    private static RoleDescriptor.IndicesPrivileges parseIndex(String roleName, XContentParser parser,
                                                               boolean allow2xFormat) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("failed to parse indices privileges for role [{}]. expected field [{}] value to " +
                    "be an array of objects, but found an array element of type [{}]", roleName, parser.currentName(), token);
        }
        String currentFieldName = null;
        String[] names = null;
        BytesReference query = null;
        String[] privileges = null;
        String[] grantedFields = null;
        String[] deniedFields = null;
        boolean allowRestrictedIndices = false;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (Fields.NAMES.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    names = new String[]{parser.text()};
                } else if (token == XContentParser.Token.START_ARRAY) {
                    names = readStringArray(roleName, parser, false);
                    if (names.length == 0) {
                        throw new ElasticsearchParseException("failed to parse indices privileges for role [{}]. [{}] cannot be an empty " +
                                "array", roleName, currentFieldName);
                    }
                } else {
                    throw new ElasticsearchParseException("failed to parse indices privileges for role [{}]. expected field [{}] " +
                            "value to be a string or an array of strings, but found [{}] instead", roleName, currentFieldName, token);
                }
            } else if (Fields.ALLOW_RESTRICTED_INDICES.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token == XContentParser.Token.VALUE_BOOLEAN) {
                    allowRestrictedIndices = parser.booleanValue();
                } else {
                    throw new ElasticsearchParseException("failed to parse indices privileges for role [{}]. expected field [{}] " +
                            "value to be a boolean, but found [{}] instead", roleName, currentFieldName, token);
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
                    throw new ElasticsearchParseException("failed to parse indices privileges for role [{}]. expected field [{}] " +
                            "value to be null, a string, an array, or an object, but found [{}] instead", roleName, currentFieldName,
                            token);
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
                                    throw new ElasticsearchParseException("failed to parse indices privileges for role [{}]. {} must not " +
                                            "be null.", roleName, Fields.GRANT_FIELDS);
                                }
                            } else if (Fields.EXCEPT_FIELDS.match(currentFieldName, parser.getDeprecationHandler())) {
                                parser.nextToken();
                                deniedFields = readStringArray(roleName, parser, true);
                                if (deniedFields == null) {
                                    throw new ElasticsearchParseException("failed to parse indices privileges for role [{}]. {} must not " +
                                            "be null.", roleName, Fields.EXCEPT_FIELDS);
                                }
                            } else {
                                throw new ElasticsearchParseException("failed to parse indices privileges for role [{}]. " +
                                        "\"{}\" only accepts options {} and {}, but got: {}",
                                        roleName, Fields.FIELD_PERMISSIONS, Fields.GRANT_FIELDS, Fields.EXCEPT_FIELDS
                                        , parser.currentName());
                            }
                        } else {
                            if (token == XContentParser.Token.END_OBJECT) {
                                throw new ElasticsearchParseException("failed to parse indices privileges for role [{}]. " +
                                        "\"{}\" must not be empty.", roleName, Fields.FIELD_PERMISSIONS);
                            } else {
                                throw new ElasticsearchParseException("failed to parse indices privileges for role [{}]. expected {} but " +
                                        "got {}.", roleName, XContentParser.Token.FIELD_NAME,
                                        token);
                            }
                        }
                    } while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT);
                } else {
                    throw new ElasticsearchParseException("failed to parse indices privileges for role [{}]. expected {} or {} but got {}" +
                            " in \"{}\".", roleName, XContentParser.Token.START_OBJECT,
                            XContentParser.Token.START_ARRAY, token, Fields.FIELD_PERMISSIONS);
                }
            } else if (Fields.PRIVILEGES.match(currentFieldName, parser.getDeprecationHandler())) {
                privileges = readStringArray(roleName, parser, true);
            } else if (Fields.FIELD_PERMISSIONS_2X.match(currentFieldName, parser.getDeprecationHandler())) {
                if (allow2xFormat) {
                    grantedFields = readStringArray(roleName, parser, true);
                } else {
                    throw new ElasticsearchParseException("[\"fields\": [...]] format has changed for field" +
                            " permissions in role [{}], use [\"{}\": {\"{}\":[...]," + "\"{}\":[...]}] instead",
                            roleName, Fields.FIELD_PERMISSIONS, Fields.GRANT_FIELDS, Fields.EXCEPT_FIELDS, roleName);
                }
            } else if (Fields.TRANSIENT_METADATA.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token == XContentParser.Token.START_OBJECT) {
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        // it is transient metadata, skip it
                    }
                } else {
                    throw new ElasticsearchParseException("failed to parse transient metadata for role [{}]. expected {} but got {}" +
                            " in \"{}\".", roleName, XContentParser.Token.START_OBJECT, token, Fields.TRANSIENT_METADATA);
                }
            } else {
                throw new ElasticsearchParseException("failed to parse indices privileges for role [{}]. unexpected field [{}]",
                        roleName, currentFieldName);
            }
        }
        if (names == null) {
            throw new ElasticsearchParseException("failed to parse indices privileges for role [{}]. missing required [{}] field",
                    roleName, Fields.NAMES.getPreferredName());
        }
        if (privileges == null) {
            throw new ElasticsearchParseException("failed to parse indices privileges for role [{}]. missing required [{}] field",
                    roleName, Fields.PRIVILEGES.getPreferredName());
        }
        if (deniedFields != null && grantedFields == null) {
            throw new ElasticsearchParseException("failed to parse indices privileges for role [{}]. {} requires {} if {} is given",
                    roleName, Fields.FIELD_PERMISSIONS, Fields.GRANT_FIELDS, Fields.EXCEPT_FIELDS);
        }
        return RoleDescriptor.IndicesPrivileges.builder()
                .indices(names)
                .privileges(privileges)
                .grantedFields(grantedFields)
                .deniedFields(deniedFields)
                .query(query)
                .allowRestrictedIndices(allowRestrictedIndices)
                .build();
    }

    private static ApplicationResourcePrivileges[] parseApplicationPrivileges(String roleName, XContentParser parser)
            throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
            throw new ElasticsearchParseException("failed to parse application privileges for role [{}]. expected field [{}] value " +
                    "to be an array, but found [{}] instead", roleName, parser.currentName(), parser.currentToken());
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
            throw new ElasticsearchParseException("failed to parse application privileges for role [{}]. expected field [{}] value to " +
                    "be an array of objects, but found an array element of type [{}]", roleName, parser.currentName(), token);
        }
        final ApplicationResourcePrivileges.Builder builder = ApplicationResourcePrivileges.PARSER.parse(parser, null);
        if (builder.hasResources() == false) {
            throw new ElasticsearchParseException("failed to parse application privileges for role [{}]. missing required [{}] field",
                    roleName, Fields.RESOURCES.getPreferredName());
        }
        if (builder.hasPrivileges() == false) {
            throw new ElasticsearchParseException("failed to parse application privileges for role [{}]. missing required [{}] field",
                    roleName, Fields.PRIVILEGES.getPreferredName());
        }
        return builder.build();
    }

    /**
     * A class representing permissions for a group of indices mapped to
     * privileges, field permissions, and a query.
     */
    public static class IndicesPrivileges implements ToXContentObject, Writeable {

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

        private IndicesPrivileges() {
        }

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

        private boolean hasDeniedFields() {
            return deniedFields != null && deniedFields.length > 0;
        }

        private boolean hasGrantedFields() {
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
                    sb.append(RoleDescriptor.Fields.GRANT_FIELDS).append("=[")
                            .append(Strings.arrayToCommaDelimitedString(grantedFields));
                    sb.append("]");
                }
                if (deniedFields == null) {
                    sb.append(", ").append(RoleDescriptor.Fields.EXCEPT_FIELDS).append("=null");
                } else {
                    sb.append(", ").append(RoleDescriptor.Fields.EXCEPT_FIELDS).append("=[")
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

            if (!Arrays.equals(indices, that.indices)) return false;
            if (allowRestrictedIndices != that.allowRestrictedIndices) return false;
            if (!Arrays.equals(privileges, that.privileges)) return false;
            if (!Arrays.equals(grantedFields, that.grantedFields)) return false;
            if (!Arrays.equals(deniedFields, that.deniedFields)) return false;
            return !(query != null ? !query.equals(that.query) : that.query != null);
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
            builder.field(RoleDescriptor.Fields.ALLOW_RESTRICTED_INDICES.getPreferredName(), allowRestrictedIndices);
            return builder.endObject();
        }

        public static class Builder {

            private IndicesPrivileges indicesPrivileges = new IndicesPrivileges();

            private Builder() {
            }

            public Builder indices(String... indices) {
                indicesPrivileges.indices = indices;
                return this;
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
        private static final ObjectParser<ApplicationResourcePrivileges.Builder, Void> PARSER = new ObjectParser<>("application",
                ApplicationResourcePrivileges::builder);

        static {
            PARSER.declareString(Builder::application, Fields.APPLICATION);
            PARSER.declareStringArray(Builder::privileges, Fields.PRIVILEGES);
            PARSER.declareStringArray(Builder::resources, Fields.RESOURCES);
        }

        private String application;
        private String[] privileges;
        private String[] resources;

        private ApplicationResourcePrivileges() {
        }

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
            StringBuilder sb = new StringBuilder(getClass().getSimpleName())
                    .append("[application=")
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

            private Builder() {
            }

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
        ParseField APPLICATIONS = new ParseField("applications");
        ParseField RUN_AS = new ParseField("run_as");
        ParseField NAMES = new ParseField("names");
        ParseField ALLOW_RESTRICTED_INDICES = new ParseField("allow_restricted_indices");
        ParseField RESOURCES = new ParseField("resources");
        ParseField QUERY = new ParseField("query");
        ParseField PRIVILEGES = new ParseField("privileges");
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
