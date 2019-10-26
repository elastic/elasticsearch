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
package org.elasticsearch.client.indices;

import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class IndexTemplateMetaData  {

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<IndexTemplateMetaData, String> PARSER = new ConstructingObjectParser<>(
        "IndexTemplateMetaData", true, (a, name) -> {
        List<Map.Entry<String, AliasMetaData>> alias = (List<Map.Entry<String, AliasMetaData>>) a[5];
        ImmutableOpenMap<String, AliasMetaData> aliasMap =
            new ImmutableOpenMap.Builder<String, AliasMetaData>()
                .putAll(alias.stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
                .build();
        return new IndexTemplateMetaData(
            name,
            (Integer) a[0],
            (Integer) a[1],
            (List<String>) a[2],
            (Settings) a[3],
            (MappingMetaData) a[4],
            aliasMap);
    });

    static {
        PARSER.declareInt(optionalConstructorArg(), new ParseField("order"));
        PARSER.declareInt(optionalConstructorArg(), new ParseField("version"));
        PARSER.declareStringArray(optionalConstructorArg(), new ParseField("index_patterns"));
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> {
            Settings.Builder templateSettingsBuilder = Settings.builder();
            templateSettingsBuilder.put(Settings.fromXContent(p));
            templateSettingsBuilder.normalizePrefix(IndexMetaData.INDEX_SETTING_PREFIX);
            return templateSettingsBuilder.build();
        }, new ParseField("settings"));
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> {
            Map<String, Object> mapping = p.map();
            if (mapping.isEmpty()) {
                return null;
            }
            return new MappingMetaData(MapperService.SINGLE_MAPPING_NAME, mapping);
        }, new ParseField("mappings"));
        PARSER.declareNamedObjects(optionalConstructorArg(),
            (p, c, name) -> new AbstractMap.SimpleEntry<>(name, AliasMetaData.Builder.fromXContent(p)), new ParseField("aliases"));
    }

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

    private final MappingMetaData mappings;

    private final ImmutableOpenMap<String, AliasMetaData> aliases;

    public IndexTemplateMetaData(String name, int order, Integer version,
                                 List<String> patterns, Settings settings,
                                 MappingMetaData mappings,
                                 ImmutableOpenMap<String, AliasMetaData> aliases) {
        if (patterns == null || patterns.isEmpty()) {
            throw new IllegalArgumentException("Index patterns must not be null or empty; got " + patterns);
        }
        this.name = name;
        this.order = order;
        this.version = version;
        this.patterns= patterns;
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

    @Nullable
    public Integer version() {
        return version;
    }

    public List<String> patterns() {
        return this.patterns;
    }

    public Settings settings() {
        return this.settings;
    }

    public MappingMetaData mappings() {
        return this.mappings;
    }

    public ImmutableOpenMap<String, AliasMetaData> aliases() {
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
        return order == that.order &&
            Objects.equals(name, that.name) &&
            Objects.equals(version, that.version) &&
            Objects.equals(patterns, that.patterns) &&
            Objects.equals(settings, that.settings) &&
            Objects.equals(mappings, that.mappings) &&
            Objects.equals(aliases, that.aliases);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, order, version, patterns, settings, mappings, aliases);
    }

    public static class Builder {

        private String name;

        private int order;

        private Integer version;

        private List<String> indexPatterns;

        private Settings settings = Settings.Builder.EMPTY_SETTINGS;

        private MappingMetaData mappings;

        private final ImmutableOpenMap.Builder<String, AliasMetaData> aliases;

        public Builder(String name) {
            this.name = name;
            mappings = null;
            aliases = ImmutableOpenMap.builder();
        }

        public Builder(IndexTemplateMetaData indexTemplateMetaData) {
            this.name = indexTemplateMetaData.name();
            order(indexTemplateMetaData.order());
            version(indexTemplateMetaData.version());
            patterns(indexTemplateMetaData.patterns());
            settings(indexTemplateMetaData.settings());

            mappings = indexTemplateMetaData.mappings();
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

        public Builder mapping(MappingMetaData mappings) {
            this.mappings = mappings;
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

        public IndexTemplateMetaData build() {
            return new IndexTemplateMetaData(name, order, version, indexPatterns, settings, mappings, aliases.build());
        }


        public static IndexTemplateMetaData fromXContent(XContentParser parser, String templateName) throws IOException {
            return PARSER.parse(parser, templateName);
        }
    }
}
