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
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link ScriptMetaData} is used to store user-defined scripts
 * as part of the {@link ClusterState} using only an id as the key.
 */
public final class ScriptMetaData implements MetaData.Custom, Writeable, ToXContentFragment {

    /**
     * A builder used to modify the currently stored scripts data held within
     * the {@link ClusterState}.  Scripts can be added or deleted, then built
     * to generate a new {@link Map} of scripts that will be used to update
     * the current {@link ClusterState}.
     */
    public static final class Builder {

        private final Map<String, StoredScriptSource> scripts;

        /**
         * @param previous The current {@link ScriptMetaData} or {@code null} if there
         *                 is no existing {@link ScriptMetaData}.
         */
        public Builder(ScriptMetaData previous) {
            this.scripts = previous == null ? new HashMap<>() :new HashMap<>(previous.scripts);
        }

        /**
         * Add a new script to the existing stored scripts based on a user-specified id.  If
         * a script with the same id already exists it will be overwritten.
         * @param id The user-specified id to use for the look up.
         * @param source The user-specified stored script data held in {@link StoredScriptSource}.
         */
        public Builder storeScript(String id, StoredScriptSource source) {
            scripts.put(id, source);

            return this;
        }

        /**
         * Delete a script from the existing stored scripts based on a user-specified id.
         * @param id The user-specified id to use for the look up.
         */
        public Builder deleteScript(String id) {
            StoredScriptSource deleted = scripts.remove(id);

            if (deleted == null) {
                throw new ResourceNotFoundException("stored script [" + id + "] does not exist and cannot be deleted");
            }

            return this;
        }

        /**
         * @return A {@link ScriptMetaData} with the updated {@link Map} of scripts.
         */
        public ScriptMetaData build() {
            return new ScriptMetaData(scripts);
        }
    }

    static final class ScriptMetadataDiff implements NamedDiff<MetaData.Custom> {

        final Diff<Map<String, StoredScriptSource>> pipelines;

        ScriptMetadataDiff(ScriptMetaData before, ScriptMetaData after) {
            this.pipelines = DiffableUtils.diff(before.scripts, after.scripts, DiffableUtils.getStringKeySerializer());
        }

        ScriptMetadataDiff(StreamInput in) throws IOException {
            pipelines = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(),
                StoredScriptSource::new, StoredScriptSource::readDiffFrom);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
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

    /**
     * Convenience method to build and return a new
     * {@link ScriptMetaData} adding the specified stored script.
     */
    static ScriptMetaData putStoredScript(ScriptMetaData previous, String id, StoredScriptSource source) {
        Builder builder = new Builder(previous);
        builder.storeScript(id, source);

        return builder.build();
    }

    /**
     * Convenience method to build and return a new
     * {@link ScriptMetaData} deleting the specified stored script.
     */
    static ScriptMetaData deleteStoredScript(ScriptMetaData previous, String id) {
        Builder builder = new ScriptMetaData.Builder(previous);
        builder.deleteScript(id);

        return builder.build();
    }

    /**
     * The type of {@link ClusterState} data.
     */
    public static final String TYPE = "stored_scripts";

    /**
     * This will parse XContent into {@link ScriptMetaData}.
     *
     * The following format will be parsed:
     *
     * {@code
     * {
     *     "<id>" : "<{@link StoredScriptSource#fromXContent(XContentParser)}>",
     *     "<id>" : "<{@link StoredScriptSource#fromXContent(XContentParser)}>",
     *     ...
     * }
     * }
     *
     * When loading from a source prior to 6.0, if multiple scripts
     * using the old namespace id format of [lang#id] are found to have the
     * same id but different languages an error will occur.
     */
    public static ScriptMetaData fromXContent(XContentParser parser) throws IOException {
        Map<String, StoredScriptSource> scripts = new HashMap<>();
        String id = null;
        StoredScriptSource source;
        StoredScriptSource exists;

        Token token = parser.currentToken();

        if (token == null) {
            token = parser.nextToken();
        }

        if (token != Token.START_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "unexpected token [" + token + "], expected [{]");
        }

        token = parser.nextToken();

        while (token != Token.END_OBJECT) {
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
                    String lang;

                    if (split == -1) {
                        throw new IllegalArgumentException("illegal stored script id [" + id + "], does not contain lang");
                    } else {
                        lang = id.substring(0, split);
                        id = id.substring(split + 1);
                        source = new StoredScriptSource(lang, parser.text(), Collections.emptyMap());
                    }

                    exists = scripts.get(id);

                    if (exists == null) {
                        scripts.put(id, source);
                    } else if (exists.getLang().equals(lang) == false) {
                        throw new IllegalArgumentException("illegal stored script, id [" + id + "] used for multiple scripts with " +
                            "different languages [" + exists.getLang() + "] and [" + lang + "]; scripts using the old namespace " +
                            "of [lang#id] as a stored script id will have to be updated to use only the new namespace of [id]");
                    }

                    id = null;

                    break;
                case START_OBJECT:
                    if (id == null) {
                        throw new ParsingException(parser.getTokenLocation(),
                            "unexpected token [" + token + "], expected [<id>, <code>, {]");
                    }

                    exists = scripts.get(id);
                    source = StoredScriptSource.fromXContent(parser);

                    if (exists == null) {
                        scripts.put(id, source);
                    } else if (exists.getLang().equals(source.getLang()) == false) {
                        throw new IllegalArgumentException("illegal stored script, id [" + id + "] used for multiple scripts with " +
                            "different languages [" + exists.getLang() + "] and [" + source.getLang() + "]; scripts using the old " +
                            "namespace of [lang#id] as a stored script id will have to be updated to use only the new namespace of [id]");
                    }

                    id = null;

                    break;
                default:
                    throw new ParsingException(parser.getTokenLocation(), "unexpected token [" + token + "], expected [<id>, <code>, {]");
            }

            token = parser.nextToken();
        }

        return new ScriptMetaData(scripts);
    }

    public static NamedDiff<MetaData.Custom> readDiffFrom(StreamInput in) throws IOException {
        return new ScriptMetadataDiff(in);
    }

    private final Map<String, StoredScriptSource> scripts;

    /**
     * Standard constructor to create metadata to store scripts.
     * @param scripts The currently stored scripts.  Must not be {@code null},
     *                use and empty {@link Map} to specify there were no
     *                previously stored scripts.
     */
    ScriptMetaData(Map<String, StoredScriptSource> scripts) {
        this.scripts = Collections.unmodifiableMap(scripts);
    }

    public ScriptMetaData(StreamInput in) throws IOException {
        Map<String, StoredScriptSource> scripts = new HashMap<>();
        StoredScriptSource source;
        int size = in.readVInt();

        for (int i = 0; i < size; i++) {
            String id = in.readString();

            // Prior to version 5.3 all scripts were stored using the deprecated namespace.
            // Split the id to find the language then use StoredScriptSource to parse the
            // expected BytesReference after which a new StoredScriptSource is created
            // with the appropriate language and options.
            if (in.getVersion().before(Version.V_5_3_0)) {
                int split = id.indexOf('#');

                if (split == -1) {
                    throw new IllegalArgumentException("illegal stored script id [" + id + "], does not contain lang");
                } else {
                    source = new StoredScriptSource(in);
                    source = new StoredScriptSource(id.substring(0, split), source.getSource(), Collections.emptyMap());
                }
            // Version 5.3+ can just be parsed normally using StoredScriptSource.
            } else {
                source = new StoredScriptSource(in);
            }

            scripts.put(id, source);
        }

        this.scripts = Collections.unmodifiableMap(scripts);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // Version 5.3+ will output the contents of the scripts' Map using
        // StoredScriptSource to stored the language, code, and options.
        if (out.getVersion().onOrAfter(Version.V_5_3_0)) {
            out.writeVInt(scripts.size());

            for (Map.Entry<String, StoredScriptSource> entry : scripts.entrySet()) {
                out.writeString(entry.getKey());
                entry.getValue().writeTo(out);
            }
        // Prior to Version 5.3, stored scripts can only be read using the deprecated
        // namespace.  Scripts using the deprecated namespace are first isolated in a
        // temporary Map, then written out.  Since all scripts will be stored using the
        // deprecated namespace, no scripts will be lost.
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



    /**
     * This will write XContent from {@link ScriptMetaData}.  The following format will be written:
     *
     * {@code
     * {
     *     "<id>" : "<{@link StoredScriptSource#toXContent(XContentBuilder, Params)}>",
     *     "<id>" : "<{@link StoredScriptSource#toXContent(XContentBuilder, Params)}>",
     *     ...
     * }
     * }
     */
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
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public EnumSet<MetaData.XContentContext> context() {
        return MetaData.ALL_CONTEXTS;
    }

    /**
     * Retrieves a stored script based on a user-specified id.
     */
    StoredScriptSource getStoredScript(String id) {
        return scripts.get(id);
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
