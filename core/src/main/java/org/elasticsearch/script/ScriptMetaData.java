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
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public final class ScriptMetaData implements MetaData.Custom {

    public final static String TYPE = "stored_scripts";
    public final static ScriptMetaData PROTO = new ScriptMetaData(Collections.emptyMap());

    private final Map<String, ScriptAsBytes> scripts;

    ScriptMetaData(Map<String, ScriptAsBytes> scripts) {
        this.scripts = scripts;
    }

    public BytesReference getScriptAsBytes(String language, String id) {
        ScriptAsBytes scriptAsBytes = scripts.get(toKey(language, id));
        if (scriptAsBytes != null) {
            return scriptAsBytes.script;
        } else {
            return null;
        }
    }

    public String getScript(String language, String id) {
        BytesReference scriptAsBytes = getScriptAsBytes(language, id);
        if (scriptAsBytes == null) {
            return null;
        }

        // Scripts can be stored via API in several ways:
        // 1) wrapped into a 'script' json object or field
        // 2) wrapped into a 'template' json object or field
        // 3) just as is
        // In order to fetch the actual script in consistent manner this parsing logic is needed:
        try (XContentParser parser = XContentHelper.createParser(scriptAsBytes);
             XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            parser.nextToken();
            parser.nextToken();
            switch (parser.currentName()) {
                case "script":
                case "template":
                    if (parser.nextToken() == Token.VALUE_STRING) {
                        return parser.text();
                    } else {
                        builder.copyCurrentStructure(parser);
                    }
                    break;
                default:
                    // There is no enclosing 'script' or 'template' object so we just need to return the script as is...
                    // because the parsers current location is already beyond the beginning we need to add a START_OBJECT:
                    builder.startObject();
                    builder.copyCurrentStructure(parser);
                    builder.endObject();
                    break;
            }
            return builder.string();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public ScriptMetaData fromXContent(XContentParser parser) throws IOException {
        Map<String, ScriptAsBytes> scripts = new HashMap<>();
        String key = null;
        for (Token token = parser.nextToken(); token != Token.END_OBJECT; token = parser.nextToken()) {
            switch (token) {
                case FIELD_NAME:
                    key = parser.currentName();
                    break;
                case START_OBJECT:
                    XContentBuilder contentBuilder = XContentBuilder.builder(parser.contentType().xContent());
                    contentBuilder.copyCurrentStructure(parser);
                    scripts.put(key, new ScriptAsBytes(contentBuilder.bytes()));
                    break;
                default:
                    throw new ParsingException(parser.getTokenLocation(), "Unexpected token [" + token + "]");
            }
        }
        return new ScriptMetaData(scripts);
    }

    @Override
    public EnumSet<MetaData.XContentContext> context() {
        return MetaData.API_AND_GATEWAY;
    }

    @Override
    public ScriptMetaData readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        Map<String, ScriptAsBytes> scripts = new HashMap<>();
        for (int i = 0; i < size; i++) {
            String languageAndId = in.readString();
            BytesReference script = in.readBytesReference();
            scripts.put(languageAndId, new ScriptAsBytes(script));
        }
        return new ScriptMetaData(scripts);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (Map.Entry<String, ScriptAsBytes> entry : scripts.entrySet()) {
            builder.rawField(entry.getKey(), entry.getValue().script);
        }
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(scripts.size());
        for (Map.Entry<String, ScriptAsBytes> entry : scripts.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeTo(out);
        }
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

    static String toKey(String language, String id) {
        if (id.contains("#")) {
            throw new IllegalArgumentException("stored script id can't contain: '#'");
        }
        if (language.contains("#")) {
            throw new IllegalArgumentException("stored script language can't contain: '#'");
        }

        return language + "#" + id;
    }

    final public static class Builder {

        private Map<String, ScriptAsBytes> scripts;

        public Builder(ScriptMetaData previous) {
            if (previous != null) {
                this.scripts = new HashMap<>(previous.scripts);
            } else {
                this.scripts = new HashMap<>();
            }
        }

        public Builder storeScript(String lang, String id, BytesReference script) {
            scripts.put(toKey(lang, id), new ScriptAsBytes(script));
            return this;
        }

        public Builder deleteScript(String lang, String id) {
            if (scripts.remove(toKey(lang, id)) == null) {
                throw new ResourceNotFoundException("Stored script with id [{}] for language [{}] does not exist", id, lang);
            }
            return this;
        }

        public ScriptMetaData build() {
            return new ScriptMetaData(Collections.unmodifiableMap(scripts));
        }
    }

    final static class ScriptMetadataDiff implements Diff<MetaData.Custom> {

        final Diff<Map<String, ScriptAsBytes>> pipelines;

        ScriptMetadataDiff(ScriptMetaData before, ScriptMetaData after) {
            this.pipelines = DiffableUtils.diff(before.scripts, after.scripts, DiffableUtils.getStringKeySerializer());
        }

        public ScriptMetadataDiff(StreamInput in) throws IOException {
            pipelines = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), new ScriptAsBytes(null));
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

    final static class ScriptAsBytes extends AbstractDiffable<ScriptAsBytes> {

        public ScriptAsBytes(BytesReference script) {
            this.script = script;
        }

        private final BytesReference script;

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBytesReference(script);
        }

        @Override
        public ScriptAsBytes readFrom(StreamInput in) throws IOException {
            return new ScriptAsBytes(in.readBytesReference());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ScriptAsBytes that = (ScriptAsBytes) o;

            return script.equals(that.script);

        }

        @Override
        public int hashCode() {
            return script.hashCode();
        }
    }
}
