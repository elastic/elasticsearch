/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.indices;

import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
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

public class IndexTemplateMetadata  {

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<IndexTemplateMetadata, String> PARSER = new ConstructingObjectParser<>(
        "IndexTemplateMetadata", true, (a, name) -> {
        List<Map.Entry<String, AliasMetadata>> alias = (List<Map.Entry<String, AliasMetadata>>) a[5];
        ImmutableOpenMap<String, AliasMetadata> aliasMap =
            new ImmutableOpenMap.Builder<String, AliasMetadata>()
                .putAll(alias.stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
                .build();
        return new IndexTemplateMetadata(
            name,
            (Integer) a[0],
            (Integer) a[1],
            (List<String>) a[2],
            (Settings) a[3],
            (MappingMetadata) a[4],
            aliasMap);
    });

    static {
        PARSER.declareInt(optionalConstructorArg(), new ParseField("order"));
        PARSER.declareInt(optionalConstructorArg(), new ParseField("version"));
        PARSER.declareStringArray(optionalConstructorArg(), new ParseField("index_patterns"));
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> {
            Settings.Builder templateSettingsBuilder = Settings.builder();
            templateSettingsBuilder.put(Settings.fromXContent(p));
            templateSettingsBuilder.normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX);
            return templateSettingsBuilder.build();
        }, new ParseField("settings"));
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> {
            Map<String, Object> mapping = p.map();
            if (mapping.isEmpty()) {
                return null;
            }
            return new MappingMetadata(MapperService.SINGLE_MAPPING_NAME, mapping);
        }, new ParseField("mappings"));
        PARSER.declareNamedObjects(optionalConstructorArg(),
            (p, c, name) -> new AbstractMap.SimpleEntry<>(name, AliasMetadata.Builder.fromXContent(p)), new ParseField("aliases"));
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

    private final MappingMetadata mappings;

    private final ImmutableOpenMap<String, AliasMetadata> aliases;

    public IndexTemplateMetadata(String name, int order, Integer version,
                                 List<String> patterns, Settings settings,
                                 MappingMetadata mappings,
                                 ImmutableOpenMap<String, AliasMetadata> aliases) {
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

    public MappingMetadata mappings() {
        return this.mappings;
    }

    public ImmutableOpenMap<String, AliasMetadata> aliases() {
        return this.aliases;
    }

    public static Builder builder(String name) {
        return new Builder(name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexTemplateMetadata that = (IndexTemplateMetadata) o;
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

        private MappingMetadata mappings;

        private final ImmutableOpenMap.Builder<String, AliasMetadata> aliases;

        public Builder(String name) {
            this.name = name;
            mappings = null;
            aliases = ImmutableOpenMap.builder();
        }

        public Builder(IndexTemplateMetadata indexTemplateMetadata) {
            this.name = indexTemplateMetadata.name();
            order(indexTemplateMetadata.order());
            version(indexTemplateMetadata.version());
            patterns(indexTemplateMetadata.patterns());
            settings(indexTemplateMetadata.settings());

            mappings = indexTemplateMetadata.mappings();
            aliases = ImmutableOpenMap.builder(indexTemplateMetadata.aliases());
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

        public Builder mapping(MappingMetadata mappings) {
            this.mappings = mappings;
            return this;
        }

        public Builder putAlias(AliasMetadata aliasMetadata) {
            aliases.put(aliasMetadata.alias(), aliasMetadata);
            return this;
        }

        public Builder putAlias(AliasMetadata.Builder aliasMetadata) {
            aliases.put(aliasMetadata.alias(), aliasMetadata.build());
            return this;
        }

        public IndexTemplateMetadata build() {
            return new IndexTemplateMetadata(name, order, version, indexPatterns, settings, mappings, aliases.build());
        }


        public static IndexTemplateMetadata fromXContent(XContentParser parser, String templateName) throws IOException {
            return PARSER.parse(parser, templateName);
        }
    }
}
