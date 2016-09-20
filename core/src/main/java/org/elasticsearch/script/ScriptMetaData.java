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

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public final class ScriptMetaData implements MetaData.Custom {

    public static final class StoredScriptSource extends AbstractDiffable<StoredScriptSource> implements ToXContent {
        public final String context;
        public final String lang;
        public final String code;

        public StoredScriptSource(String context, String lang, String code) {
            this.context = context;
            this.lang = lang;
            this.code = code;
        }

        public static StoredScriptSource staticReadFrom(StreamInput in) throws IOException {
            return new StoredScriptSource(in.readOptionalString(), in.readString(), in.readString());
        }

        @Override
        public StoredScriptSource readFrom(StreamInput in) throws IOException {
            return new StoredScriptSource(in.readOptionalString(), in.readString(), in.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(context);
            out.writeString(lang);
            out.writeString(code);
        }

        public static StoredScriptSource fromXContent(XContentParser parser) throws IOException {
            String context = null;
            String lang = null;
            String code = null;

            for (Token token = parser.nextToken(); token != Token.END_OBJECT; token = parser.nextToken()) {
                if (parser.currentToken() == Token.FIELD_NAME) {
                    String name = parser.currentName();

                    parser.nextToken();

                    if (parser.currentToken() == Token.VALUE_STRING) {
                        if ("context".equals(name)) {
                            context = parser.text();
                        } else if ("lang".equals(name)) {
                            lang = parser.text();
                        } else if ("code".equals(name)) {
                            code = parser.text();
                        } else {
                            throw new ParsingException(parser.getTokenLocation(),
                                "unexpected token [" + parser.currentToken() + "], expected one of [context, lang, code]");
                        }
                    } else {
                        throw new ParsingException(parser.getTokenLocation(),
                            "unexpected token [" + parser.currentToken() + "], expected the value for one of [" + name + "]");
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                        "unexpected token [" + parser.currentToken() + "], expected one of [context, lang, code]");
                }
            }

            return new StoredScriptSource(context, lang, code);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (context != null) {
                builder.field("context", context);
            }

            builder.field("lang", lang);
            builder.field("code", code);

            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            StoredScriptSource that = (StoredScriptSource)o;

            if (context != null ? !context.equals(that.context) : that.context != null) return false;
            if (lang != null ? !lang.equals(that.lang) : that.lang != null) return false;
            return code.equals(that.code);

        }

        @Override
        public int hashCode() {
            int result = context != null ? context.hashCode() : 0;
            result = 31 * result + (lang != null ? lang.hashCode() : 0);
            result = 31 * result + code.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "StoredScriptSource{" +
                "context='" + context + '\'' +
                ", lang='" + lang + '\'' +
                ", code='" + code + '\'' +
                '}';
        }
    }

    static final class ScriptMetadataDiff implements Diff<MetaData.Custom> {

        final Diff<Map<String, StoredScriptSource>> pipelines;

        ScriptMetadataDiff(ScriptMetaData before, ScriptMetaData after) {
            this.pipelines = DiffableUtils.diff(before.scripts, after.scripts, DiffableUtils.getStringKeySerializer());
        }

        public ScriptMetadataDiff(StreamInput in) throws IOException {
            pipelines = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), new StoredScriptSource(null, null, null));
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

    public static final String TYPE = "stored_scripts";
    public static final ScriptMetaData PROTO = new ScriptMetaData(Collections.emptyMap());

    static ClusterState storeScript(ClusterState state, String id, StoredScriptSource source) {
        ScriptMetaData scriptMetadata = state.metaData().custom(ScriptMetaData.TYPE);
        Map<String, StoredScriptSource> scripts;

        if (scriptMetadata == null) {
            scripts = new HashMap<>();
        } else {
            scripts = new HashMap<>(scriptMetadata.scripts);
        }

        scripts.put(id, source);

        MetaData.Builder metaDataBuilder = MetaData.builder(state.getMetaData())
            .putCustom(ScriptMetaData.TYPE, new ScriptMetaData(scripts));

        return ClusterState.builder(state).metaData(metaDataBuilder).build();
    }

    static ClusterState deleteScript(ClusterState state, String id) {
        ScriptMetaData scriptMetadata = state.metaData().custom(ScriptMetaData.TYPE);

        if (scriptMetadata == null) {
            throw new ResourceNotFoundException("stored script with id [" + id + "] does not exist");
        }

        Map<String, StoredScriptSource> scripts = new HashMap<>(scriptMetadata.scripts);

        if (scripts.remove(id) == null) {
            throw new ResourceNotFoundException("stored script with id [" + id + "] does not exist");
        }

        MetaData.Builder metaDataBuilder = MetaData.builder(state.getMetaData())
            .putCustom(ScriptMetaData.TYPE, new ScriptMetaData(scripts));
        return ClusterState.builder(state).metaData(metaDataBuilder).build();
    }

    static StoredScriptSource getScript(ClusterState state, String id) {
        ScriptMetaData scriptMetadata = state.metaData().custom(ScriptMetaData.TYPE);

        if (scriptMetadata != null) {
            return scriptMetadata.getScript(id);
        } else {
            return null;
        }
    }

    private final Map<String, StoredScriptSource> scripts;

    ScriptMetaData(Map<String, StoredScriptSource> scripts) {
        this.scripts = Collections.unmodifiableMap(new HashMap<>(scripts));
    }

    public StoredScriptSource getScript(String id) {
        return scripts.get(id);
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public EnumSet<MetaData.XContentContext> context() {
        return MetaData.API_AND_GATEWAY;
    }

    @Override
    public ScriptMetaData readFrom(StreamInput in) throws IOException {
        Map<String, StoredScriptSource> scripts = new HashMap<>();
        int size = in.readVInt();

        for (int script = 0; script < size; ++script) {
            String id = in.readString();
            StoredScriptSource source = StoredScriptSource.staticReadFrom(in);

            scripts.put(id, source);
        }

        return new ScriptMetaData(scripts);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(scripts.size());

        for (Map.Entry<String, StoredScriptSource> entry : scripts.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (Map.Entry<String, StoredScriptSource> entry : scripts.entrySet()) {
            builder.startObject(entry.getKey());
            entry.getValue().toXContent(builder, params);
            builder.endObject();
        }
        return builder;
    }

    @Override
    public ScriptMetaData fromXContent(XContentParser parser) throws IOException {
        Map<String, StoredScriptSource> scripts = new HashMap<>();
        String id = null;

        for (Token token = parser.nextToken(); token != Token.END_OBJECT; token = parser.nextToken()) {
            switch (token) {
                case FIELD_NAME:
                    id = parser.currentName();

                    break;
                case START_OBJECT:
                    if (id == null) {
                        throw new ParsingException(parser.getTokenLocation(),
                            "unexpected token [" + token + "], no stored script id specified");
                    }

                    scripts.put(id, StoredScriptSource.fromXContent(parser));

                    id = null;

                    if (parser.currentToken() != Token.END_OBJECT) {
                        throw new ParsingException(parser.getTokenLocation(), "unexpected token [" + token + "], expected end object [}]");
                    }

                    break;
                default:
                    throw new ParsingException(parser.getTokenLocation(), "unexpected token [" + token + "], expected one of [id, }]");
            }
        }
        return new ScriptMetaData(scripts);
    }

    @Override
    public Diff<MetaData.Custom> diff(MetaData.Custom before) {
        return new ScriptMetadataDiff((ScriptMetaData) before, this);
    }

    @Override
    public Diff<MetaData.Custom> readDiffFrom(StreamInput in) throws IOException {
        return new ScriptMetadataDiff(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ScriptMetaData other = (ScriptMetaData) o;
        return scripts.equals(other.scripts);
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
