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
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A template consists of optional settings, mappings, or alias configuration for an index, however,
 * it is entirely independent from an index. It's a building block forming part of a regular index
 * template and a {@link ComponentTemplate}.
 */
public class Template extends AbstractDiffable<Template> implements ToXContentObject {
    private static final ParseField SETTINGS = new ParseField("settings");
    private static final ParseField MAPPINGS = new ParseField("mappings");
    private static final ParseField ALIASES = new ParseField("aliases");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<Template, Void> PARSER = new ConstructingObjectParser<>("template", false,
        a -> new Template((Settings) a[0], (CompressedXContent) a[1], (Map<String, AliasMetadata>) a[2]));

    static {
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> Settings.fromXContent(p), SETTINGS);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) ->
            new CompressedXContent(Strings.toString(XContentFactory.jsonBuilder().map(p.mapOrdered()))), MAPPINGS);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
            Map<String, AliasMetadata> aliasMap = new HashMap<>();
            while ((p.nextToken()) != XContentParser.Token.END_OBJECT) {
                AliasMetadata alias = AliasMetadata.Builder.fromXContent(p);
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
    private final Map<String, AliasMetadata> aliases;

    public Template(@Nullable Settings settings, @Nullable CompressedXContent mappings, @Nullable Map<String, AliasMetadata> aliases) {
        this.settings = settings;
        this.mappings = mappings;
        this.aliases = aliases;
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
            this.aliases = in.readMap(StreamInput::readString, AliasMetadata::new);
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

    public Map<String, AliasMetadata> aliases() {
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
            out.writeMap(this.aliases, StreamOutput::writeString, (stream, aliasMetadata) -> aliasMetadata.writeTo(stream));
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
                builder.map(reduceMapping(uncompressedMapping));
            }
        }
        if (this.aliases != null) {
            builder.startObject(ALIASES.getPreferredName());
            for (AliasMetadata alias : this.aliases.values()) {
                AliasMetadata.Builder.toXContent(alias, builder, params);
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> reduceMapping(Map<String, Object> mapping) {
        if (mapping.size() == 1 && MapperService.SINGLE_MAPPING_NAME.equals(mapping.keySet().iterator().next())) {
            return (Map<String, Object>) mapping.values().iterator().next();
        } else {
            return mapping;
        }
    }
}
