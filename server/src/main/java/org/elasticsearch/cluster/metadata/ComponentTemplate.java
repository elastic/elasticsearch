/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A component template is a re-usable template as well as metadata about the template. Each
 * component template is expected to be valid on its own. For example, if a component template
 * contains a field "foo", it's expected to contain all the necessary settings/mappings/etc for the
 * "foo" field. These component templates make up the individual pieces composing an index template.
 */
public class ComponentTemplate extends AbstractDiffable<ComponentTemplate> implements ToXContentObject {
    private static final ParseField TEMPLATE = new ParseField("template");
    private static final ParseField VERSION = new ParseField("version");
    private static final ParseField METADATA = new ParseField("_meta");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ComponentTemplate, Void> PARSER =
        new ConstructingObjectParser<>("component_template", false,
            a -> new ComponentTemplate((Template) a[0], (Long) a[1], (Map<String, Object>) a[2]));

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), Template.PARSER, TEMPLATE);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), VERSION);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), METADATA);
    }

    private final Template template;
    @Nullable
    private final Long version;
    @Nullable
    private final Map<String, Object> metadata;

    static Diff<ComponentTemplate> readComponentTemplateDiffFrom(StreamInput in) throws IOException {
        return AbstractDiffable.readDiffFrom(ComponentTemplate::new, in);
    }

    public static ComponentTemplate parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public ComponentTemplate(Template template, @Nullable Long version, @Nullable Map<String, Object> metadata) {
        this.template = template;
        this.version = version;
        this.metadata = metadata;
    }

    public ComponentTemplate(StreamInput in) throws IOException {
        this.template = new Template(in);
        this.version = in.readOptionalVLong();
        if (in.readBoolean()) {
            this.metadata = in.readMap();
        } else {
            this.metadata = null;
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        this.template.writeTo(out);
        out.writeOptionalVLong(this.version);
        if (this.metadata == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeMap(this.metadata);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(template, version, metadata);
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
        return Objects.equals(template, other.template) &&
            Objects.equals(version, other.version) &&
            Objects.equals(metadata, other.metadata);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TEMPLATE.getPreferredName(), this.template);
        if (this.version != null) {
            builder.field(VERSION.getPreferredName(), this.version);
        }
        if (this.metadata != null) {
            builder.field(METADATA.getPreferredName(), this.metadata);
        }
        builder.endObject();
        return builder;
    }

    static class Template extends AbstractDiffable<Template> implements ToXContentObject {
        private static final ParseField SETTINGS = new ParseField("settings");
        private static final ParseField MAPPINGS = new ParseField("mappings");
        private static final ParseField ALIASES = new ParseField("aliases");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Template, Void> PARSER = new ConstructingObjectParser<>("template", false,
            a -> new Template((Settings) a[0], (CompressedXContent) a[1], (Map<String, AliasMetaData>) a[2]));

        static {
            PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> Settings.fromXContent(p), SETTINGS);
            PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) ->
                new CompressedXContent(Strings.toString(XContentFactory.jsonBuilder().map(p.mapOrdered()))), MAPPINGS);
            PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
                Map<String, AliasMetaData> aliasMap = new HashMap<>();
                while ((p.nextToken()) != XContentParser.Token.END_OBJECT) {
                    AliasMetaData alias = AliasMetaData.Builder.fromXContent(p);
                    aliasMap.put(alias.alias(), alias);
                }
                return aliasMap;
            }, ALIASES);
        }

        @Nullable
        private final Settings settings;
        @Nullable
        private final CompressedXContent mappings;
        @Nullable
        private final Map<String, AliasMetaData> aliases;

        Template(@Nullable Settings settings, @Nullable CompressedXContent mappings, @Nullable Map<String, AliasMetaData> aliases) {
            this.settings = settings;
            this.mappings = mappings;
            this.aliases = aliases;
        }

        Template(StreamInput in) throws IOException {
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
                this.aliases = in.readMap(StreamInput::readString, AliasMetaData::new);
            } else {
                this.aliases = null;
            }
        }

        public Settings settings() {
            return settings;
        }

        public CompressedXContent mappings() {
            return mappings;
        }

        public Map<String, AliasMetaData> aliases() {
            return aliases;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (this.settings == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                Settings.writeSettingsToStream(this.settings, out);
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
                out.writeMap(this.aliases, StreamOutput::writeString, (stream, aliasMetaData) -> aliasMetaData.writeTo(stream));
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(settings, mappings, aliases);
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
            return Objects.equals(settings, other.settings) &&
                Objects.equals(mappings, other.mappings) &&
                Objects.equals(aliases, other.aliases);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (this.settings != null) {
                builder.startObject(SETTINGS.getPreferredName());
                this.settings.toXContent(builder, params);
                builder.endObject();
            }
            if (this.mappings != null) {
                Map<String, Object> uncompressedMapping =
                    XContentHelper.convertToMap(new BytesArray(this.mappings.uncompressed()), true, XContentType.JSON).v2();
                if (uncompressedMapping.size() > 0) {
                    builder.field(MAPPINGS.getPreferredName());
                    builder.map(uncompressedMapping);
                }
            }
            if (this.aliases != null) {
                builder.startObject(ALIASES.getPreferredName());
                for (AliasMetaData alias : this.aliases.values()) {
                    AliasMetaData.Builder.toXContent(alias, builder, params);
                }
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }
    }
}
