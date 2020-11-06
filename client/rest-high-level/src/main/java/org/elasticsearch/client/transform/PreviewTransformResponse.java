/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.transform;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class PreviewTransformResponse {

    public static class GeneratedDestIndexSettings {
        static final ParseField MAPPINGS = new ParseField("mappings");
        private static final ParseField SETTINGS = new ParseField("settings");
        private static final ParseField ALIASES = new ParseField("aliases");

        private final Map<String, Object> mappings;
        private final Settings settings;
        private final Set<Alias> aliases;

        private static final ConstructingObjectParser<GeneratedDestIndexSettings, Void> PARSER = new ConstructingObjectParser<>(
            "transform_preview_generated_dest_index",
            true,
            args -> {
                @SuppressWarnings("unchecked")
                Map<String, Object> mappings = (Map<String, Object>) args[0];
                Settings settings = (Settings) args[1];
                @SuppressWarnings("unchecked")
                Set<Alias> aliases = (Set<Alias>) args[2];

                return new GeneratedDestIndexSettings(mappings, settings, aliases);
            }
        );

        static {
            PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.mapOrdered(), MAPPINGS);
            PARSER.declareObject(optionalConstructorArg(), (p, c) -> Settings.fromXContent(p), SETTINGS);
            PARSER.declareObject(optionalConstructorArg(), (p, c) -> {
                Set<Alias> aliases = new HashSet<>();
                while ((p.nextToken()) != XContentParser.Token.END_OBJECT) {
                    aliases.add(Alias.fromXContent(p));
                }
                return aliases;
            }, ALIASES);
        }

        public GeneratedDestIndexSettings(Map<String, Object> mappings, Settings settings, Set<Alias> aliases) {
            this.mappings = mappings == null ? Collections.emptyMap() : Collections.unmodifiableMap(mappings);
            this.settings = settings == null ? Settings.EMPTY : settings;
            this.aliases = aliases == null ? Collections.emptySet() : Collections.unmodifiableSet(aliases);
        }

        public Map<String, Object> getMappings() {
            return mappings;
        }

        public Settings getSettings() {
            return settings;
        }

        public Set<Alias> getAliases() {
            return aliases;
        }

        public static GeneratedDestIndexSettings fromXContent(final XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }

            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }

            GeneratedDestIndexSettings other = (GeneratedDestIndexSettings) obj;
            return Objects.equals(other.mappings, mappings)
                && Objects.equals(other.settings, settings)
                && Objects.equals(other.aliases, aliases);
        }

        @Override
        public int hashCode() {
            return Objects.hash(mappings, settings, aliases);
        }
    }

    public static final ParseField PREVIEW = new ParseField("preview");
    public static final ParseField GENERATED_DEST_INDEX_SETTINGS = new ParseField("generated_dest_index");

    private final List<Map<String, Object>> docs;
    private final GeneratedDestIndexSettings generatedDestIndexSettings;

    private static final ConstructingObjectParser<PreviewTransformResponse, Void> PARSER = new ConstructingObjectParser<>(
        "data_frame_transform_preview",
        true,
        args -> {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> docs = (List<Map<String, Object>>) args[0];
            GeneratedDestIndexSettings generatedDestIndex = (GeneratedDestIndexSettings) args[1];

            // ensure generatedDestIndex is not null
            if (generatedDestIndex == null) {
                // BWC parsing the output from nodes < 7.7
                @SuppressWarnings("unchecked")
                Map<String, Object> mappings = (Map<String, Object>) args[2];
                generatedDestIndex = new GeneratedDestIndexSettings(mappings, null, null);
            }

            return new PreviewTransformResponse(docs, generatedDestIndex);
        }
    );
    static {
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> p.mapOrdered(), PREVIEW);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> GeneratedDestIndexSettings.fromXContent(p), GENERATED_DEST_INDEX_SETTINGS);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.mapOrdered(), GeneratedDestIndexSettings.MAPPINGS);
    }

    public PreviewTransformResponse(List<Map<String, Object>> docs, GeneratedDestIndexSettings generatedDestIndexSettings) {
        this.docs = docs;
        this.generatedDestIndexSettings = generatedDestIndexSettings;
    }

    public List<Map<String, Object>> getDocs() {
        return docs;
    }

    public GeneratedDestIndexSettings getGeneratedDestIndexSettings() {
        return generatedDestIndexSettings;
    }

    public Map<String, Object> getMappings() {
        return generatedDestIndexSettings.getMappings();
    }

    public Settings getSettings() {
        return generatedDestIndexSettings.getSettings();
    }

    public Set<Alias> getAliases() {
        return generatedDestIndexSettings.getAliases();
    }

    public CreateIndexRequest getCreateIndexRequest(String index) {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
        createIndexRequest.aliases(generatedDestIndexSettings.getAliases());
        createIndexRequest.settings(generatedDestIndexSettings.getSettings());
        createIndexRequest.mapping(generatedDestIndexSettings.getMappings());

        return createIndexRequest;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }

        PreviewTransformResponse other = (PreviewTransformResponse) obj;
        return Objects.equals(other.docs, docs) && Objects.equals(other.generatedDestIndexSettings, generatedDestIndexSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(docs, generatedDestIndexSettings);
    }

    public static PreviewTransformResponse fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
