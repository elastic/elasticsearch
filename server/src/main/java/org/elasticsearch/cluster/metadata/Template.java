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
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A template consists of optional settings, mappings, alias or lifecycle configuration for an index or data stream, however,
 * it is entirely independent of an index or data stream. It's a building block forming part of a regular index
 * template and a {@link ComponentTemplate}.
 */
public class Template implements SimpleDiffable<Template>, ToXContentObject {

    private static final ParseField SETTINGS = new ParseField("settings");
    private static final ParseField MAPPINGS = new ParseField("mappings");
    private static final ParseField ALIASES = new ParseField("aliases");
    private static final ParseField LIFECYCLE = new ParseField("lifecycle");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<Template, Void> PARSER = new ConstructingObjectParser<>(
        "template",
        false,
        a -> new Template((Settings) a[0], (CompressedXContent) a[1], (Map<String, AliasMetadata>) a[2], (DataStreamLifecycle) a[3])
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> Settings.fromXContent(p), SETTINGS);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
            XContentParser.Token token = p.currentToken();
            if (token == XContentParser.Token.VALUE_STRING) {
                return new CompressedXContent(Base64.getDecoder().decode(p.text()));
            } else if (token == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
                return new CompressedXContent(p.binaryValue());
            } else if (token == XContentParser.Token.START_OBJECT) {
                return new CompressedXContent(Strings.toString(XContentFactory.jsonBuilder().map(p.mapOrdered())));
            } else {
                throw new IllegalArgumentException("Unexpected token: " + token);
            }
        }, MAPPINGS, ObjectParser.ValueType.VALUE_OBJECT_ARRAY);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
            Map<String, AliasMetadata> aliasMap = new HashMap<>();
            while ((p.nextToken()) != XContentParser.Token.END_OBJECT) {
                AliasMetadata alias = AliasMetadata.Builder.fromXContent(p);
                aliasMap.put(alias.alias(), alias);
            }
            return aliasMap;
        }, ALIASES);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> DataStreamLifecycle.fromXContent(p), LIFECYCLE);
    }

    @Nullable
    private final Settings settings;
    @Nullable
    private final CompressedXContent mappings;
    @Nullable
    private final Map<String, AliasMetadata> aliases;

    @Nullable
    private final DataStreamLifecycle lifecycle;

    public Template(
        @Nullable Settings settings,
        @Nullable CompressedXContent mappings,
        @Nullable Map<String, AliasMetadata> aliases,
        @Nullable DataStreamLifecycle lifecycle
    ) {
        this.settings = settings;
        this.mappings = mappings;
        this.aliases = aliases;
        this.lifecycle = lifecycle;
    }

    public Template(@Nullable Settings settings, @Nullable CompressedXContent mappings, @Nullable Map<String, AliasMetadata> aliases) {
        this(settings, mappings, aliases, null);
    }

    public Template(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            this.settings = Settings.readSettingsFromStream(in);
        } else {
            this.settings = null;
        }
        if (in.readBoolean()) {
            this.mappings = CompressedXContent.readCompressedString(in);
        } else {
            this.mappings = null;
        }
        if (in.readBoolean()) {
            this.aliases = in.readMap(AliasMetadata::new);
        } else {
            this.aliases = null;
        }
        if (in.getTransportVersion().onOrAfter(DataStreamLifecycle.ADDED_ENABLED_FLAG_VERSION)) {
            this.lifecycle = in.readOptionalWriteable(DataStreamLifecycle::new);
        } else if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
            boolean isExplicitNull = in.readBoolean();
            if (isExplicitNull) {
                this.lifecycle = DataStreamLifecycle.newBuilder().enabled(false).build();
            } else {
                this.lifecycle = in.readOptionalWriteable(DataStreamLifecycle::new);
            }
        } else {
            this.lifecycle = null;
        }
    }

    @Nullable
    public Settings settings() {
        return settings;
    }

    @Nullable
    public CompressedXContent mappings() {
        return mappings;
    }

    @Nullable
    public Map<String, AliasMetadata> aliases() {
        return aliases;
    }

    @Nullable
    public DataStreamLifecycle lifecycle() {
        return lifecycle;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (this.settings == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            this.settings.writeTo(out);
        }
        if (this.mappings == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            this.mappings.writeTo(out);
        }
        if (this.aliases == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeMap(this.aliases, StreamOutput::writeWriteable);
        }
        if (out.getTransportVersion().onOrAfter(DataStreamLifecycle.ADDED_ENABLED_FLAG_VERSION)) {
            out.writeOptionalWriteable(lifecycle);
        } else if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
            boolean isExplicitNull = lifecycle != null && lifecycle.isEnabled() == false;
            out.writeBoolean(isExplicitNull);
            if (isExplicitNull == false) {
                out.writeOptionalWriteable(lifecycle);
            }
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(settings, mappings, aliases, lifecycle);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        Template other = (Template) obj;
        return Objects.equals(settings, other.settings)
            && mappingsEquals(this.mappings, other.mappings)
            && Objects.equals(aliases, other.aliases)
            && Objects.equals(lifecycle, other.lifecycle);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return toXContent(builder, params, null, null);
    }

    /**
     * Converts the template to XContent and passes the RolloverConditions, when provided, to the lifecycle.
     */
    public XContentBuilder toXContent(
        XContentBuilder builder,
        Params params,
        @Nullable RolloverConfiguration rolloverConfiguration,
        @Nullable DataStreamGlobalRetention globalRetention
    ) throws IOException {
        builder.startObject();
        if (this.settings != null) {
            builder.startObject(SETTINGS.getPreferredName());
            this.settings.toXContent(builder, params);
            builder.endObject();
        }
        if (this.mappings != null) {
            String context = params.param(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_API);
            boolean binary = params.paramAsBoolean("binary", false);
            if (Metadata.CONTEXT_MODE_API.equals(context) || binary == false) {
                Map<String, Object> uncompressedMapping = XContentHelper.convertToMap(this.mappings.uncompressed(), true, XContentType.JSON)
                    .v2();
                if (uncompressedMapping.size() > 0) {
                    builder.field(MAPPINGS.getPreferredName());
                    builder.map(reduceMapping(uncompressedMapping));
                }
            } else {
                builder.field(MAPPINGS.getPreferredName(), mappings.compressed());
            }
        }
        if (this.aliases != null) {
            builder.startObject(ALIASES.getPreferredName());
            for (AliasMetadata alias : this.aliases.values()) {
                AliasMetadata.Builder.toXContent(alias, builder, params);
            }
            builder.endObject();
        }
        if (this.lifecycle != null) {
            builder.field(LIFECYCLE.getPreferredName());
            lifecycle.toXContent(builder, params, rolloverConfiguration, globalRetention);
        }
        builder.endObject();
        return builder;
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> reduceMapping(Map<String, Object> mapping) {
        if (mapping.size() == 1 && MapperService.SINGLE_MAPPING_NAME.equals(mapping.keySet().iterator().next())) {
            return (Map<String, Object>) mapping.values().iterator().next();
        } else {
            return mapping;
        }
    }

    static boolean mappingsEquals(CompressedXContent m1, CompressedXContent m2) {
        if (m1 == m2) {
            return true;
        }

        if (m1 == null || m2 == null) {
            return false;
        }

        if (m1.equals(m2)) {
            return true;
        }

        Map<String, Object> thisUncompressedMapping = reduceMapping(
            XContentHelper.convertToMap(m1.uncompressed(), true, XContentType.JSON).v2()
        );
        Map<String, Object> otherUncompressedMapping = reduceMapping(
            XContentHelper.convertToMap(m2.uncompressed(), true, XContentType.JSON).v2()
        );
        return Maps.deepEquals(thisUncompressedMapping, otherUncompressedMapping);
    }
}
