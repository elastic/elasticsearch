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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class IndexTemplateMetaData extends AbstractDiffable<IndexTemplateMetaData> implements ToXContentFragment {

    private final String name;

    private final int order;

    /**
     * The version is an arbitrary number managed by the user so that they can easily and quickly verify the existence of a given template.
     * Expected usage:
     * <pre><code>
     * PUT /_template/my_template
     * {
     *   "index_patterns": ["my_index-*"],
     *   "mappings": { ... },
     *   "version": 1
     * }
     * </code></pre>
     * Then, some process from the user can occasionally verify that the template exists with the appropriate version without having to
     * check the template's content:
     * <pre><code>
     * GET /_template/my_template?filter_path=*.version
     * </code></pre>
     */
    @Nullable
    private final Integer version;

    private final List<String> patterns;

    private final Settings settings;

    // the mapping source should always include the type as top level
    private final ImmutableOpenMap<String, CompressedXContent> mappings;

    private final ImmutableOpenMap<String, AliasMetaData> aliases;

    public IndexTemplateMetaData(String name, int order, Integer version,
                                 List<String> patterns, Settings settings,
                                 ImmutableOpenMap<String, CompressedXContent> mappings,
                                 ImmutableOpenMap<String, AliasMetaData> aliases) {
        if (patterns == null || patterns.isEmpty()) {
            throw new IllegalArgumentException("Index patterns must not be null or empty; got " + patterns);
        }
        this.name = name;
        this.order = order;
        this.version = version;
        this.patterns = patterns;
        this.settings = settings;
        this.mappings = mappings;
        this.aliases = aliases;
    }

    public String name() {
        return this.name;
    }

    public int order() {
        return this.order;
    }

    public int getOrder() {
        return order();
    }

    @Nullable
    public Integer getVersion() {
        return version();
    }

    @Nullable
    public Integer version() {
        return version;
    }

    public String getName() {
        return this.name;
    }

    public List<String> patterns() {
        return this.patterns;
    }

    public Settings settings() {
        return this.settings;
    }

    public CompressedXContent mappings() {
        if (this.mappings.isEmpty()) {
            return null;
        }
        return this.mappings.iterator().next().value;
    }

    public CompressedXContent getMappings() {
        return this.mappings();
    }

    public ImmutableOpenMap<String, AliasMetaData> aliases() {
        return this.aliases;
    }

    public ImmutableOpenMap<String, AliasMetaData> getAliases() {
        return this.aliases;
    }

    public static Builder builder(String name) {
        return new Builder(name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexTemplateMetaData that = (IndexTemplateMetaData) o;

        if (order != that.order) return false;
        if (!mappings.equals(that.mappings)) return false;
        if (!name.equals(that.name)) return false;
        if (!settings.equals(that.settings)) return false;
        if (!patterns.equals(that.patterns)) return false;

        return Objects.equals(version, that.version);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + order;
        result = 31 * result + Objects.hashCode(version);
        result = 31 * result + patterns.hashCode();
        result = 31 * result + settings.hashCode();
        result = 31 * result + mappings.hashCode();
        return result;
    }

    public static IndexTemplateMetaData readFrom(StreamInput in) throws IOException {
        Builder builder = new Builder(in.readString());
        builder.order(in.readInt());
        builder.patterns(in.readStringList());
        builder.settings(Settings.readSettingsFromStream(in));
        int mappingsSize = in.readVInt();
        for (int i = 0; i < mappingsSize; i++) {
            builder.putMapping(in.readString(), CompressedXContent.readCompressedString(in));
        }
        int aliasesSize = in.readVInt();
        for (int i = 0; i < aliasesSize; i++) {
            AliasMetaData aliasMd = new AliasMetaData(in);
            builder.putAlias(aliasMd);
        }
        builder.version(in.readOptionalVInt());
        return builder.build();
    }

    public static Diff<IndexTemplateMetaData> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(IndexTemplateMetaData::readFrom, in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeInt(order);
        out.writeStringCollection(patterns);
        Settings.writeSettingsToStream(settings, out);
        out.writeVInt(mappings.size());
        for (ObjectObjectCursor<String, CompressedXContent> cursor : mappings) {
            out.writeString(cursor.key);
            cursor.value.writeTo(out);
        }
        out.writeVInt(aliases.size());
        for (ObjectCursor<AliasMetaData> cursor : aliases.values()) {
            cursor.value.writeTo(out);
        }
        out.writeOptionalVInt(version);
    }

    @Override
    public String toString() {
        try {
            XContentBuilder builder = JsonXContent.contentBuilder();
            this.toXContent(builder, ToXContent.EMPTY_PARAMS);
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static class Builder {

        private static final Set<String> VALID_FIELDS = Sets.newHashSet(
            "order", "mappings", "settings", "index_patterns", "aliases", "version");

        private String name;

        private int order;

        private Integer version;

        private List<String> indexPatterns;

        private Settings settings = Settings.Builder.EMPTY_SETTINGS;

        private final ImmutableOpenMap.Builder<String, CompressedXContent> mappings;

        private final ImmutableOpenMap.Builder<String, AliasMetaData> aliases;

        public Builder(String name) {
            this.name = name;
            mappings = ImmutableOpenMap.builder();
            aliases = ImmutableOpenMap.builder();
        }

        public Builder(IndexTemplateMetaData indexTemplateMetaData) {
            this.name = indexTemplateMetaData.name();
            order(indexTemplateMetaData.order());
            version(indexTemplateMetaData.version());
            patterns(indexTemplateMetaData.patterns());
            settings(indexTemplateMetaData.settings());

            mappings = ImmutableOpenMap.builder(indexTemplateMetaData.mappings);
            aliases = ImmutableOpenMap.builder(indexTemplateMetaData.aliases());
        }

        public Builder order(int order) {
            this.order = order;
            return this;
        }

        public Builder version(Integer version) {
            this.version = version;
            return this;
        }

        public Builder patterns(List<String> indexPatterns) {
            this.indexPatterns = indexPatterns;
            return this;
        }


        public Builder settings(Settings.Builder settings) {
            this.settings = settings.build();
            return this;
        }

        public Builder settings(Settings settings) {
            this.settings = settings;
            return this;
        }

        public Builder putMapping(String mappingType, CompressedXContent mappingSource) {
            mappings.put(mappingType, mappingSource);
            return this;
        }

        public Builder putMapping(String mappingType, String mappingSource) throws IOException {
            mappings.put(mappingType, new CompressedXContent(mappingSource));
            return this;
        }

        public Builder putAlias(AliasMetaData aliasMetaData) {
            aliases.put(aliasMetaData.alias(), aliasMetaData);
            return this;
        }

        public Builder putAlias(AliasMetaData.Builder aliasMetaData) {
            aliases.put(aliasMetaData.alias(), aliasMetaData.build());
            return this;
        }

        public void putAliases(List<AliasMetaData> aliases) {
            for (AliasMetaData alias : aliases) {
                putAlias(alias);
            }
        }

        public IndexTemplateMetaData build() {
            return new IndexTemplateMetaData(name, order, version, indexPatterns, settings, mappings.build(), aliases.build());
        }

    }

    public static final String INCLUDE_TYPE_NAME = "include_type_name";
    public static final String INCLUDE_TEMPLATE_NAME = "include_template_name";

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {

        boolean includeTypeName = params.paramAsBoolean(INCLUDE_TYPE_NAME, false);
        boolean includeTemplateName = params.paramAsBoolean(INCLUDE_TEMPLATE_NAME, true);

        if (includeTemplateName) {
            builder.startObject(this.name());
        }

        builder.field("order", this.order());
        if (this.version() != null) {
            builder.field("version", this.version());
        }
        builder.field("index_patterns", this.patterns());

        builder.startObject("settings");
        this.settings().toXContent(builder, params);
        builder.endObject();

        CompressedXContent m = this.mappings();
        if (m != null) {
            Map<String, Object> documentMapping = XContentHelper.convertToMap(new BytesArray(m.uncompressed()), true).v2();
            if (includeTypeName == false) {
                documentMapping = reduceMapping(documentMapping);
            }
            builder.field("mappings");
            builder.map(documentMapping);
        } else {
            builder.startObject("mappings").endObject();
        }

        builder.startObject("aliases");
        for (ObjectCursor<AliasMetaData> cursor : this.aliases().values()) {
            AliasMetaData.Builder.toXContent(cursor.value, builder, params);
        }
        builder.endObject();

        if (includeTemplateName) {
            builder.endObject();
        }

        return builder;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> reduceMapping(Map<String, Object> mapping) {
        assert mapping.keySet().size() == 1;
        return (Map<String, Object>) mapping.values().iterator().next();
    }

    private static final ObjectParser<Builder, String> PARSER = ObjectParser.fromBuilder("index_template", Builder::new);

    static {
        PARSER.declareObject(Builder::settings,
            (p, c) -> Settings.builder().put(Settings.fromXContent(p)).normalizePrefix(IndexMetaData.INDEX_SETTING_PREFIX).build(),
            new ParseField("settings"));
        PARSER.declareObject((builder, map) -> {
            if (map.isEmpty() == false) {
                String type = map.keySet().iterator().next();
                builder.putMapping(type, map.get(type));
            }
        }, (p, c) -> parseMappings(p), new ParseField("mappings"));
        PARSER.declareNamedObjects(Builder::putAliases, (p, c, n) -> AliasMetaData.Builder.fromXContent(p), new ParseField("aliases"));
        PARSER.declareStringArray(Builder::patterns, new ParseField("index_patterns"));
        PARSER.declareInt(Builder::order, new ParseField("order"));
        PARSER.declareInt(Builder::version, new ParseField("version"));
    }

    // TODO it would be really nice if we could just copy bytes here, there's no need to parse and then
    // re-stream the mappings
    private static Map<String, CompressedXContent> parseMappings(XContentParser parser) throws IOException {
        String mappingType = null;
        String mappings = null;
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                mappingType = parser.currentName();
            } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                if (mappingType == null) {
                    throw new XContentParseException(parser.getTokenLocation(), "Expected a mapping type");
                }
                mappings = Strings.toString(XContentFactory.jsonBuilder().map(Map.of(mappingType, parser.mapOrdered())));
            }
        }
        if (mappings == null) {
            return Collections.emptyMap();
        }
        return Map.of(mappingType, new CompressedXContent(mappings));
    }

    public static IndexTemplateMetaData fromXContent(XContentParser parser, String templateName) throws IOException {
        return PARSER.parse(parser, templateName).build();
    }

}
