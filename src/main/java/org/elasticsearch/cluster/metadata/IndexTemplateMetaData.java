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

import com.google.common.collect.Sets;
import org.elasticsearch.cluster.*;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.loader.SettingsLoader;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.warmer.IndexWarmersMetaData;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class IndexTemplateMetaData extends NamedCompositeClusterStatePart<IndexClusterStatePart> implements NamedClusterStatePart {

    public static String TYPE = "template";

    public static Factory FACTORY = new Factory();

    public static final String SETTINGS_TYPE = "settings";

    public static final ClusterStateSettingsPart.Factory SETTINGS_FACTORY = new ClusterStateSettingsPart.Factory(SETTINGS_TYPE, API_GATEWAY_SNAPSHOT);

    public static final String ALIASES_TYPE = "aliases";

    public static final MapClusterStatePart.Factory<AliasMetaData> ALIASES_FACTORY = new MapClusterStatePart.Factory<>(ALIASES_TYPE, AliasMetaData.FACTORY, API_GATEWAY_SNAPSHOT);

    static {
        FACTORY.registerFactory(SETTINGS_TYPE, SETTINGS_FACTORY);
        FACTORY.registerFactory(CompressedMappingMetaData.TYPE, CompressedMappingMetaData.FACTORY);
        FACTORY.registerFactory(ALIASES_TYPE, ALIASES_FACTORY);
        // register non plugin custom metadata
        FACTORY.registerFactory(IndexWarmersMetaData.TYPE, IndexWarmersMetaData.FACTORY);
    }

    public static class Factory extends NamedCompositeClusterStatePart.AbstractFactory<IndexClusterStatePart, IndexTemplateMetaData> {

        @Override
        public NamedCompositeClusterStatePart.Builder<IndexClusterStatePart, IndexTemplateMetaData> builder(String key) {
            return new Builder(key);
        }

        @Override
        public NamedCompositeClusterStatePart.Builder<IndexClusterStatePart, IndexTemplateMetaData> builder(IndexTemplateMetaData part) {
            return new Builder(part);
        }

        @Override
        protected void valuesPartWriteTo(IndexTemplateMetaData indexTemplateMetaData, StreamOutput out) throws IOException {
            out.writeInt(indexTemplateMetaData.order());
            out.writeString(indexTemplateMetaData.template());
        }

        @Override
        protected void valuesPartToXContent(IndexTemplateMetaData indexTemplateMetaData, XContentBuilder builder, Params params) throws IOException {
            builder.field("order", indexTemplateMetaData.order());
            builder.field("template", indexTemplateMetaData.template());
        }

        @Override
        public IndexTemplateMetaData fromXContent(XContentParser parser, LocalContext context) throws IOException {
            if (parser.currentToken() == null) { // fresh parser? move to the first token
                parser.nextToken();
            }
            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {  // on a start object move to next token
                parser.nextToken();
            }
            return Builder.fromXContent(parser, parser.currentName());
        }

        @Override
        public EnumSet<XContentContext> context() {
            return API_GATEWAY_SNAPSHOT;
        }


        @Override
        public String partType() {
            return TYPE;
        }
    }


    private final String name;

    private final int order;

    private final String template;

    private final Settings settings;

    // the mapping source should always include the type as top level
    private final ImmutableOpenMap<String, CompressedString> mappings;

    private final ImmutableOpenMap<String, AliasMetaData> aliases;

    public IndexTemplateMetaData(String name, int order, String template, ImmutableOpenMap<String, IndexClusterStatePart> parts) {
        super(parts);
        this.name = name;
        this.order = order;
        this.template = template;
        this.settings = parts.containsKey(SETTINGS_TYPE) ? ((ClusterStateSettingsPart)get(SETTINGS_TYPE)).getSettings() : ImmutableSettings.EMPTY;
        this.mappings = parts.containsKey(CompressedMappingMetaData.TYPE) ? ((CompressedMappingMetaData)get(CompressedMappingMetaData.TYPE)).mappings() : ImmutableOpenMap.<String, CompressedString>of();
        this.aliases = parts.containsKey(ALIASES_TYPE) ? ((MapClusterStatePart<AliasMetaData>)get(ALIASES_TYPE)).parts() : ImmutableOpenMap.<String, AliasMetaData>of();
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

    public String getName() {
        return this.name;
    }

    public String template() {
        return this.template;
    }

    public String getTemplate() {
        return this.template;
    }

    public Settings settings() {
        return this.settings;
    }

    public Settings getSettings() {
        return settings();
    }

    public ImmutableOpenMap<String, CompressedString> mappings() {
        return this.mappings;
    }

    public ImmutableOpenMap<String, CompressedString> getMappings() {
        return this.mappings;
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
        if (!template.equals(that.template)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + order;
        result = 31 * result + template.hashCode();
        result = 31 * result + settings.hashCode();
        result = 31 * result + mappings.hashCode();
        return result;
    }

    public static class Builder extends NamedCompositeClusterStatePart.Builder<IndexClusterStatePart, IndexTemplateMetaData> {

        private static final Set<String> VALID_FIELDS = Sets.newHashSet("template", "order");
        static {
            VALID_FIELDS.addAll(IndexMetaData.FACTORY.availableFactories());
            VALID_FIELDS.remove("aliases");
        }

        private String name;

        private int order;

        private String template;

        private Settings settings = ImmutableSettings.Builder.EMPTY_SETTINGS;

        private final ImmutableOpenMap.Builder<String, CompressedString> mappings;

        private final ImmutableOpenMap.Builder<String, AliasMetaData> aliases;

        public Builder(String name) {
            this.name = name;
            mappings = ImmutableOpenMap.builder();
            aliases = ImmutableOpenMap.builder();
        }

        public Builder(IndexTemplateMetaData indexTemplateMetaData) {
            this.name = indexTemplateMetaData.name();
            order(indexTemplateMetaData.order());
            template(indexTemplateMetaData.template());
            settings(indexTemplateMetaData.settings());
            mappings = ImmutableOpenMap.builder(indexTemplateMetaData.mappings());
            aliases = ImmutableOpenMap.builder(indexTemplateMetaData.aliases());
            parts.putAll(indexTemplateMetaData.parts());
            parts.remove(SETTINGS_TYPE);
            parts.remove(CompressedMappingMetaData.TYPE);
            parts.remove(ALIASES_TYPE);
        }

        public Builder order(int order) {
            this.order = order;
            return this;
        }

        public Builder template(String template) {
            this.template = template;
            return this;
        }

        public String template() {
            return template;
        }

        public String getKey() {
            return name;
        }

        public Builder settings(Settings.Builder settings) {
            this.settings = settings.build();
            return this;
        }

        public Builder settings(Settings settings) {
            this.settings = settings;
            return this;
        }

        public Builder removeMapping(String mappingType) {
            mappings.remove(mappingType);
            return this;
        }

        public Builder putMapping(String mappingType, CompressedString mappingSource) throws IOException {
            mappings.put(mappingType, mappingSource);
            return this;
        }

        public Builder putMapping(String mappingType, String mappingSource) throws IOException {
            mappings.put(mappingType, new CompressedString(mappingSource));
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

        public Builder putCustom(String type, IndexClusterStatePart customIndexMetaData) {
            this.parts.put(type, customIndexMetaData);
            return this;
        }

        public Builder removeCustom(String type) {
            this.parts.remove(type);
            return this;
        }

        public IndexClusterStatePart getCustom(String type) {
            return this.parts.get(type);
        }

        public IndexTemplateMetaData build() {
            return new IndexTemplateMetaData(name, order, template, buildParts(settings, mappings.build(), aliases.build(), parts.build()));
        }

        @Override
        public void parseValuePart(XContentParser parser, String currentFieldName, LocalContext context) throws IOException {
            if ("template".equals(currentFieldName)) {
                template(parser.text());
            } else if ("order".equals(currentFieldName)) {
                order(parser.intValue());
            }
        }

        @Override
        public void readValuePartsFrom(StreamInput in, LocalContext context) throws IOException {
            order(in.readInt());
            template(in.readString());
        }

        @Override
        public void writeValuePartsTo(StreamOutput out) throws IOException {
            out.writeInt(order);
            out.writeString(template);
        }

        private static ImmutableOpenMap<String, IndexClusterStatePart> buildParts(Settings settings,
                                                                                  ImmutableOpenMap<String, CompressedString> mappings,
                                                                                  ImmutableOpenMap<String, AliasMetaData> aliases,
                                                                                  ImmutableOpenMap<String, IndexClusterStatePart> parts) {
            ImmutableOpenMap.Builder<String, IndexClusterStatePart> builder = ImmutableOpenMap.builder();
            builder.put(SETTINGS_TYPE, SETTINGS_FACTORY.fromSettings(settings));
            builder.put(CompressedMappingMetaData.TYPE, CompressedMappingMetaData.FACTORY.fromOpenMap(mappings));
            builder.put(ALIASES_TYPE, ALIASES_FACTORY.fromOpenMap(aliases));
            builder.putAll(parts);
            return builder.build();
        }

        @SuppressWarnings("unchecked")
        public static void toXContent(IndexTemplateMetaData indexTemplateMetaData, XContentBuilder builder, ToXContent.Params params) throws IOException {
            FACTORY.toXContent(indexTemplateMetaData, builder, params);
        }

        public static IndexTemplateMetaData fromXContent(XContentParser parser, String templateName) throws IOException {
            Builder builder = new Builder(templateName);

            String currentFieldName = skipTemplateName(parser);
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("settings".equals(currentFieldName)) {
                        ImmutableSettings.Builder templateSettingsBuilder = ImmutableSettings.settingsBuilder();
                        for (Map.Entry<String, String> entry : SettingsLoader.Helper.loadNestedFromMap(parser.mapOrdered()).entrySet()) {
                            if (!entry.getKey().startsWith("index.")) {
                                templateSettingsBuilder.put("index." + entry.getKey(), entry.getValue());
                            } else {
                                templateSettingsBuilder.put(entry.getKey(), entry.getValue());
                            }
                        }
                        builder.settings(templateSettingsBuilder.build());
                    } else {
                        IndexClusterStatePart.Factory<IndexClusterStatePart> factory = IndexMetaData.FACTORY.lookupFactory(currentFieldName);
                        if (factory == null) {
                            //TODO warn
                            parser.skipChildren();
                        } else {
                            builder.putCustom(currentFieldName, factory.fromXContent(parser, null));
                        }
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    IndexClusterStatePart.Factory<IndexClusterStatePart> factory = IndexTemplateMetaData.FACTORY.lookupFactory(currentFieldName);
                    if (factory == null) {
                        //TODO warn
                        parser.skipChildren();
                    } else {
                        builder.putCustom(currentFieldName, factory.fromXContent(parser, null));
                    }
                } else if (token.isValue()) {
                    builder.parseValuePart(parser, currentFieldName, null);
                }
            }
            return builder.build();
        }

        private static String skipTemplateName(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.nextToken();
            if (token != null && token == XContentParser.Token.START_OBJECT) {
                token = parser.nextToken();
                if (token == XContentParser.Token.FIELD_NAME) {
                    String currentFieldName = parser.currentName();
                    if (VALID_FIELDS.contains(currentFieldName)) {
                        return currentFieldName;
                    } else {
                        // we just hit the template name, which should be ignored and we move on
                        parser.nextToken();
                    }
                }
            }

            return null;
        }

        public static IndexTemplateMetaData readFrom(StreamInput in) throws IOException {
            return FACTORY.readFrom(in, null);
        }

        public static void writeTo(IndexTemplateMetaData indexTemplateMetaData, StreamOutput out) throws IOException {
            FACTORY.writeTo(indexTemplateMetaData, out);
        }
    }

    @Override
    public String key() {
        return name;
    }

}
