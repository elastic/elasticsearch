/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * A component template is a re-usable {@link Template} as well as metadata about the template. Each
 * component template is expected to be valid on its own. For example, if a component template
 * contains a field "foo", it's expected to contain all the necessary settings/mappings/etc for the
 * "foo" field. These component templates make up the individual pieces composing an index template.
 */
public class ComponentTemplate implements SimpleDiffable<ComponentTemplate>, ToXContentObject {
    private static final ParseField TEMPLATE = new ParseField("template");
    private static final ParseField VERSION = new ParseField("version");
    private static final ParseField METADATA = new ParseField("_meta");
    private static final ParseField DEPRECATED = new ParseField("deprecated");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<ComponentTemplate, Void> PARSER = new ConstructingObjectParser<>(
        "component_template",
        false,
        a -> new ComponentTemplate((Template) a[0], (Long) a[1], (Map<String, Object>) a[2], (Boolean) a[3])
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), Template.PARSER, TEMPLATE);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), VERSION);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), METADATA);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), DEPRECATED);
    }

    private final Template template;
    @Nullable
    private final Long version;
    @Nullable
    private final Map<String, Object> metadata;
    @Nullable
    private final Boolean deprecated;

    static Diff<ComponentTemplate> readComponentTemplateDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(ComponentTemplate::new, in);
    }

    public static ComponentTemplate parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public ComponentTemplate(Template template, @Nullable Long version, @Nullable Map<String, Object> metadata) {
        this(template, version, metadata, null);
    }

    public ComponentTemplate(
        Template template,
        @Nullable Long version,
        @Nullable Map<String, Object> metadata,
        @Nullable Boolean deprecated
    ) {
        this.template = template;
        this.version = version;
        this.metadata = metadata;
        this.deprecated = deprecated;
    }

    public ComponentTemplate(StreamInput in) throws IOException {
        this.template = new Template(in);
        this.version = in.readOptionalVLong();
        if (in.readBoolean()) {
            this.metadata = in.readGenericMap();
        } else {
            this.metadata = null;
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            this.deprecated = in.readOptionalBoolean();
        } else {
            deprecated = null;
        }
    }

    public Template template() {
        return template;
    }

    @Nullable
    public Long version() {
        return version;
    }

    @Nullable
    public Map<String, Object> metadata() {
        return metadata;
    }

    public Boolean deprecated() {
        return deprecated;
    }

    public boolean isDeprecated() {
        return Boolean.TRUE.equals(deprecated);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        this.template.writeTo(out);
        out.writeOptionalVLong(this.version);
        if (this.metadata == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeGenericMap(this.metadata);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            out.writeOptionalBoolean(this.deprecated);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(template, version, metadata, deprecated);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        ComponentTemplate other = (ComponentTemplate) obj;
        return Objects.equals(template, other.template)
            && Objects.equals(version, other.version)
            && Objects.equals(metadata, other.metadata)
            && Objects.equals(deprecated, other.deprecated);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return toXContent(builder, params, null);
    }

    /**
     * Converts the component template to XContent and passes the RolloverConditions, when provided, to the template.
     */
    public XContentBuilder toXContent(XContentBuilder builder, Params params, @Nullable RolloverConfiguration rolloverConfiguration)
        throws IOException {
        builder.startObject();
        builder.field(TEMPLATE.getPreferredName());
        this.template.toXContent(builder, params, rolloverConfiguration);
        if (this.version != null) {
            builder.field(VERSION.getPreferredName(), this.version);
        }
        if (this.metadata != null) {
            builder.field(METADATA.getPreferredName(), this.metadata);
        }
        if (this.deprecated != null) {
            builder.field(DEPRECATED.getPreferredName(), this.deprecated);
        }
        builder.endObject();
        return builder;
    }
}
