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
package org.elasticsearch.script;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public final class ScriptMetaData implements MetaData.Custom {

    public static final class Builder {

        private final Map<String, StoredScriptSource> scripts;

        public Builder(ScriptMetaData previous) {
            this.scripts = previous == null ? new HashMap<>() :new HashMap<>(previous.scripts);
        }

        public Builder storeScript(String id, StoredScriptSource source) {
            StoredScriptSource previous = scripts.put(id, source);
            scripts.put(source.getLang() + "#" + id, source);

            if (previous != null && previous.getLang().equals(source.getLang()) == false) {
                DEPRECATION_LOGGER.deprecated("stored script [" + id + "] already exists using a different lang " +
                    "[" + previous.getLang() + "], the new namespace for stored scripts will only use (id) instead of (lang, id)");
            }

            return this;
        }

        public Builder deleteScript(String id, String lang) {
            StoredScriptSource source = scripts.get(id);

            if (lang == null) {
                if (source == null) {
                    throw new ResourceNotFoundException("stored script [" + id + "] does not exist and cannot be deleted");
                }

                lang = source.getLang();
            }

            if (source != null) {
                if (lang.equals(source.getLang())) {
                    scripts.remove(id);
                }
            }

            source = scripts.get(lang + "#" + id);

            if (source == null) {
                throw new ResourceNotFoundException(
                    "stored script [" + id + "] using lang [" + lang + "] does not exist and cannot be deleted");
            }

            scripts.remove(lang + "#" + id);

            return this;
        }

        public ScriptMetaData build() {
            return new ScriptMetaData(Collections.unmodifiableMap(scripts));
        }
    }

    static final class ScriptMetadataDiff implements Diff<MetaData.Custom> {

        final Diff<Map<String, StoredScriptSource>> pipelines;

        ScriptMetadataDiff(ScriptMetaData before, ScriptMetaData after) {
            this.pipelines = DiffableUtils.diff(before.scripts, after.scripts, DiffableUtils.getStringKeySerializer());
        }

        ScriptMetadataDiff(StreamInput in) throws IOException {
            pipelines = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), new StoredScriptSource());
        }

        @Override
        public MetaData.Custom apply(MetaData.Custom part) {
            return new ScriptMetaData(pipelines.apply(((ScriptMetaData) part).scripts));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            pipelines.writeTo(out);
        }
    }

    private static final Logger LOGGER = ESLoggerFactory.getLogger(ScriptMetaData.class);
    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(LOGGER);

    public static final String TYPE = "stored_scripts";
    public static final ScriptMetaData PROTO = new ScriptMetaData(Collections.emptyMap());

    static ClusterState putStoredScript(ClusterState currentState, String id, StoredScriptSource source) {
        ScriptMetaData scriptMetadata = currentState.metaData().custom(TYPE);

        Builder scriptMetadataBuilder = new Builder(scriptMetadata);
        scriptMetadataBuilder.storeScript(id, source);

        MetaData.Builder metaDataBuilder = MetaData.builder(currentState.getMetaData())
            .putCustom(ScriptMetaData.TYPE, scriptMetadataBuilder.build());

        return ClusterState.builder(currentState).metaData(metaDataBuilder).build();
    }

    static ClusterState deleteStoredScript(ClusterState currentState, String id, String lang) {
        ScriptMetaData scriptMetadata = currentState.metaData().custom(ScriptMetaData.TYPE);

        ScriptMetaData.Builder scriptMetadataBuilder = new ScriptMetaData.Builder(scriptMetadata);
        scriptMetadataBuilder.deleteScript(id, lang);

        MetaData.Builder metaDataBuilder = MetaData.builder(currentState.getMetaData())
            .putCustom(ScriptMetaData.TYPE, scriptMetadataBuilder.build());

        return ClusterState.builder(currentState).metaData(metaDataBuilder).build();
    }

    private final Map<String, StoredScriptSource> scripts;

    ScriptMetaData(Map<String, StoredScriptSource> scripts) {
        this.scripts = scripts;
    }

    @Override
    public ScriptMetaData readFrom(StreamInput in) throws IOException {
        Map<String, StoredScriptSource> scripts = new HashMap<>();
        StoredScriptSource source;
        int size = in.readVInt();

        for (int i = 0; i < size; i++) {
            String id = in.readString();

            if (in.getVersion().before(Version.V_5_2_0_UNRELEASED)) {
                int split = id.indexOf('#');

                if (split == -1) {
                    throw new IllegalArgumentException("illegal stored script id [" + id + "], does not contain lang");
                } else {
                    source = new StoredScriptSource(in);
                    source = new StoredScriptSource(id.substring(split + 1), source.getCode(), Collections.emptyMap());
                }
            } else {
                source = new StoredScriptSource(in);
            }

            scripts.put(id, source);
        }

        return new ScriptMetaData(scripts);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_5_2_0_UNRELEASED)) {
            out.write(scripts.size());

            for (Map.Entry<String, StoredScriptSource> entry : scripts.entrySet()) {
                out.writeString(entry.getKey());
                entry.getValue().writeTo(out);
            }
        } else {
            Map<String, StoredScriptSource> filtered = new HashMap<>();

            for (Map.Entry<String, StoredScriptSource> entry : scripts.entrySet()) {
                if (entry.getKey().contains("#")) {
                    filtered.put(entry.getKey(), entry.getValue());
                }
            }

            out.writeVInt(filtered.size());

            for (Map.Entry<String, StoredScriptSource> entry : filtered.entrySet()) {
                out.writeString(entry.getKey());
                entry.getValue().writeTo(out);
            }
        }
    }

    @Override
    public ScriptMetaData fromXContent(XContentParser parser) throws IOException {
        Map<String, StoredScriptSource> scripts = new HashMap<>();
        String id = null;
        StoredScriptSource source;

        for (Token token = parser.nextToken(); token != Token.END_OBJECT; token = parser.nextToken()) {
            switch (token) {
                case FIELD_NAME:
                    id = parser.currentName();
                    break;
                case VALUE_STRING:
                    if (id == null) {
                        throw new ParsingException(parser.getTokenLocation(),
                            "unexpected token [" + token + "], expected [<id>, <code>, {]");
                    }

                    int split = id.indexOf('#');

                    if (split == -1) {
                        throw new IllegalArgumentException("illegal stored script id [" + id + "], does not contain lang");
                    } else {
                        source = new StoredScriptSource(id.substring(split + 1), parser.text(), Collections.emptyMap());
                    }

                    scripts.put(id, source);

                    id = null;

                    break;
                case START_OBJECT:
                    if (id == null) {
                        throw new ParsingException(parser.getTokenLocation(),
                            "unexpected token [" + token + "], expected [<id>, <code>, {]");
                    }

                    source = StoredScriptSource.fromXContent(parser);
                    scripts.put(id, source);

                    break;
                default:
                    throw new ParsingException(parser.getTokenLocation(), "unexpected token [" + token + "], expected [<id>, <code>, {]");
            }
        }

        return new ScriptMetaData(scripts);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (Map.Entry<String, StoredScriptSource> entry : scripts.entrySet()) {
            builder.field(entry.getKey());
            entry.getValue().toXContent(builder, params);
        }

        return builder;
    }

    @Override
    public Diff<MetaData.Custom> diff(MetaData.Custom before) {
        return new ScriptMetadataDiff((ScriptMetaData)before, this);
    }

    @Override
    public Diff<MetaData.Custom> readDiffFrom(StreamInput in) throws IOException {
        return new ScriptMetadataDiff(in);
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public EnumSet<MetaData.XContentContext> context() {
        return MetaData.ALL_CONTEXTS;
    }

    public StoredScriptSource getStoredScript(String id, String lang) {
        if (lang == null) {
            return scripts.get(id);
        } else {
            return scripts.get(lang + "#" + id);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ScriptMetaData that = (ScriptMetaData)o;

        return scripts.equals(that.scripts);

    }

    @Override
    public int hashCode() {
        return scripts.hashCode();
    }

    @Override
    public String toString() {
        return "ScriptMetaData{" +
            "scripts=" + scripts +
            '}';
    }
}
