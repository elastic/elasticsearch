/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.support.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.ExpressionModel;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.ExpressionParser;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.RoleMapperExpression;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A representation of a single role-mapping for use in NativeRoleMappingStore.
 * Logically, this represents a set of roles that should be applied to any user where a boolean
 * expression evaluates to <code>true</code>.
 *
 * @see RoleMapperExpression
 * @see ExpressionParser
 */
public class ExpressionRoleMapping implements ToXContentObject, Writeable {

    private static final ObjectParser<Builder, String> PARSER = new ObjectParser<>("role-mapping", Builder::new);

    /**
     * The Upgrade API added a 'type' field when converting from 5 to 6.
     * We don't use it, but we need to skip it if it exists.
     */
    private static final String UPGRADE_API_TYPE_FIELD = "type";

    static {
        PARSER.declareStringArray(Builder::roles, Fields.ROLES);
        PARSER.declareObjectArray(Builder::roleTemplates, (parser, ctx) -> TemplateRoleName.parse(parser), Fields.ROLE_TEMPLATES);
        PARSER.declareField(Builder::rules, ExpressionParser::parseObject, Fields.RULES, ValueType.OBJECT);
        PARSER.declareField(Builder::metadata, XContentParser::map, Fields.METADATA, ValueType.OBJECT);
        PARSER.declareBoolean(Builder::enabled, Fields.ENABLED);
        BiConsumer<Builder, String> ignored = (b, v) -> {
        };
        // skip the doc_type and type fields in case we're parsing directly from the index
        PARSER.declareString(ignored, new ParseField(NativeRoleMappingStoreField.DOC_TYPE_FIELD));
        PARSER.declareString(ignored, new ParseField(UPGRADE_API_TYPE_FIELD));
    }

    private final String name;
    private final RoleMapperExpression expression;
    private final List<String> roles;
    private final List<TemplateRoleName> roleTemplates ;
    private final Map<String, Object> metadata;
    private final boolean enabled;

    public ExpressionRoleMapping(String name, RoleMapperExpression expr, List<String> roles, List<TemplateRoleName> templates,
                                 Map<String, Object> metadata, boolean enabled) {
        this.name = name;
        this.expression = expr;
        this.roles = roles == null ? Collections.emptyList() : roles;
        this.roleTemplates = templates == null ? Collections.emptyList() : templates;
        this.metadata = metadata;
        this.enabled = enabled;
    }

    public ExpressionRoleMapping(StreamInput in) throws IOException {
        this.name = in.readString();
        this.enabled = in.readBoolean();
        this.roles = in.readStringList();
        if (in.getVersion().onOrAfter(Version.V_7_2_0)) {
            this.roleTemplates = in.readList(TemplateRoleName::new);
        } else {
            this.roleTemplates = Collections.emptyList();
        }
        this.expression = ExpressionParser.readExpression(in);
        this.metadata = in.readMap();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeBoolean(enabled);
        out.writeStringCollection(roles);
        if (out.getVersion().onOrAfter(Version.V_7_2_0)) {
            out.writeList(roleTemplates);
        }
        ExpressionParser.writeExpression(expression, out);
        out.writeMap(metadata);
    }

    /**
     * The name of this mapping. The name exists for the sole purpose of providing a meaningful identifier for each mapping, so that it may
     * be referred to for update, retrieval or deletion. The name does not affect the set of roles that a mapping provides.
     */
    public String getName() {
        return name;
    }

    /**
     * The expression that determines whether the roles in this mapping should be applied to any given user.
     * If the expression
     * {@link RoleMapperExpression#match(ExpressionModel) matches} a
     * org.elasticsearch.xpack.security.authc.support.UserRoleMapper.UserData user, then the user should be assigned this mapping's
     * {@link #getRoles() roles}
     */
    public RoleMapperExpression getExpression() {
        return expression;
    }

    /**
     * The list of {@link RoleDescriptor roles} (specified by name) that should be assigned to users
     * that match the {@link #getExpression() expression} in this mapping.
     */
    public List<String> getRoles() {
        return Collections.unmodifiableList(roles);
    }

    /**
     * The list of {@link RoleDescriptor roles} (specified by a {@link TemplateRoleName template} that evaluates to one or more names)
     * that should be assigned to users that match the {@link #getExpression() expression} in this mapping.
     */
    public List<TemplateRoleName> getRoleTemplates() {
        return Collections.unmodifiableList(roleTemplates);
    }

    /**
     * Meta-data for this mapping. This exists for external systems of user to track information about this mapping such as where it was
     * sourced from, when it was loaded, etc.
     * This is not used within the mapping process, and does not affect whether the expression matches, nor which roles are assigned.
     */
    public Map<String, Object> getMetadata() {
        return Collections.unmodifiableMap(metadata);
    }

    /**
     * Whether this mapping is enabled. Mappings that are not enabled are not applied to users.
     */
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "<" + name + " ; " + roles + "/" + roleTemplates + " = " + Strings.toString(expression) + ">";
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ExpressionRoleMapping that = (ExpressionRoleMapping) o;
        return this.enabled == that.enabled &&
            Objects.equals(this.name, that.name) &&
            Objects.equals(this.expression, that.expression) &&
            Objects.equals(this.roles, that.roles) &&
            Objects.equals(this.roleTemplates, that.roleTemplates) &&
            Objects.equals(this.metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, expression, roles, roleTemplates, metadata, enabled);
    }

    /**
     * Parse an {@link ExpressionRoleMapping} from the provided <em>XContent</em>
     */
    public static ExpressionRoleMapping parse(String name, BytesReference source, XContentType xContentType) throws IOException {
        final NamedXContentRegistry registry = NamedXContentRegistry.EMPTY;
        try (InputStream stream = source.streamInput();
             XContentParser parser = xContentType.xContent()
                .createParser(registry, LoggingDeprecationHandler.INSTANCE, stream)) {
            return parse(name, parser);
        }
    }

    /**
     * Parse an {@link ExpressionRoleMapping} from the provided <em>XContent</em>
     */
    public static ExpressionRoleMapping parse(String name, XContentParser parser) throws IOException {
        try {
            final Builder builder = PARSER.parse(parser, name);
            return builder.build(name);
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw new ParsingException(parser.getTokenLocation(), e.getMessage(), e);
        }
    }

    /**
     * Converts this {@link ExpressionRoleMapping} into <em>XContent</em> that is compatible with
     *  the format handled by {@link #parse(String, BytesReference, XContentType)}.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return toXContent(builder, params, false);
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params, boolean indexFormat) throws IOException {
        builder.startObject();
        builder.field(Fields.ENABLED.getPreferredName(), enabled);
        if (roles.isEmpty() == false) {
            builder.startArray(Fields.ROLES.getPreferredName());
            for (String r : roles) {
                builder.value(r);
            }
            builder.endArray();
        }
        if (roleTemplates.isEmpty() == false) {
            builder.startArray(Fields.ROLE_TEMPLATES.getPreferredName());
            for (TemplateRoleName r : roleTemplates) {
                builder.value(r);
            }
            builder.endArray();
        }
        builder.field(Fields.RULES.getPreferredName());
        expression.toXContent(builder, params);

        builder.field(Fields.METADATA.getPreferredName(), metadata);

        if (indexFormat) {
            builder.field(NativeRoleMappingStoreField.DOC_TYPE_FIELD, NativeRoleMappingStoreField.DOC_TYPE_ROLE_MAPPING);
        }
        return builder.endObject();
    }

    public Set<String> getRoleNames(ScriptService scriptService, ExpressionModel model) {
        return Stream.concat(this.roles.stream(),
            this.roleTemplates.stream()
                .flatMap(r -> r.getRoleNames(scriptService, model).stream())
        ).collect(Collectors.toSet());
    }

    /**
     * Used to facilitate the use of {@link ObjectParser} (via {@link #PARSER}).
     */
    private static class Builder {
        private RoleMapperExpression rules;
        private List<String> roles;
        private List<TemplateRoleName> roleTemplates;
        private Map<String, Object> metadata = Collections.emptyMap();
        private Boolean enabled;

        Builder rules(RoleMapperExpression expression) {
            this.rules = expression;
            return this;
        }

        Builder roles(List<String> roles) {
            this.roles = new ArrayList<>(roles);
            return this;
        }

        Builder roleTemplates(List<TemplateRoleName> templates) {
            this.roleTemplates = new ArrayList<>(templates);
            return this;
        }

        Builder metadata(Map<String, Object> metadata) {
            this.metadata = metadata;
            return this;
        }

        Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        private ExpressionRoleMapping build(String name) {
            if (roles == null && roleTemplates == null) {
                throw missingField(name, Fields.ROLES);
            }
            if (rules == null) {
                throw missingField(name, Fields.RULES);
            }
            if (enabled == null) {
                throw missingField(name, Fields.ENABLED);
            }
            return new ExpressionRoleMapping(name, rules, roles, roleTemplates, metadata, enabled);
        }

        private IllegalStateException missingField(String id, ParseField field) {
            return new IllegalStateException("failed to parse role-mapping [" + id + "]. missing field [" + field + "]");
        }
    }

    public interface Fields {
        ParseField ROLES = new ParseField("roles");
        ParseField ROLE_TEMPLATES = new ParseField("role_templates");
        ParseField ENABLED = new ParseField("enabled");
        ParseField RULES = new ParseField("rules");
        ParseField METADATA = new ParseField("metadata");
    }
}
