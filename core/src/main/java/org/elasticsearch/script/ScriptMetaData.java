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
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.ToXContent;
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
 * as part of the {@link ClusterState}.  Currently scripts can
 * be stored as part of the new namespace for a stored script where
 * only an id is used or as part of the deprecated namespace where
 * both a language and an id are used.
 */
public final class ScriptMetaData implements MetaData.Custom, Writeable, ToXContent {

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
         * Add a new script to the existing stored scripts.  The script will be added under
         * both the new namespace and the deprecated namespace, so that look ups under
         * the deprecated namespace will continue to work.  Should a script already exist under
         * the new namespace using a different language, it will be replaced and a deprecation
         * warning will be issued.  The replaced script will still exist under the deprecated
         * namespace and can continue to be looked up this way until it is deleted.
         * <p>
         * Take for example script 'A' with lang 'L0' and data 'D0'.  If we add script 'A' to the
         * empty set, the scripts {@link Map} will be ["A" -- D0, "A#L0" -- D0].  If a script
         * 'A' with lang 'L1' and data 'D1' is then added, the scripts {@link Map} will be
         * ["A" -- D1, "A#L1" -- D1, "A#L0" -- D0].
         * @param id The user-specified id to use for the look up.
         * @param source The user-specified stored script data held in {@link StoredScriptSource}.
         */
        public Builder storeScript(String id, StoredScriptSource source) {
            StoredScriptSource previous = scripts.put(id, source);
            scripts.put(source.getLang() + "#" + id, source);

            if (previous != null && previous.getLang().equals(source.getLang()) == false) {
                DEPRECATION_LOGGER.deprecated("stored script [" + id + "] already exists using a different lang " +
                    "[" + previous.getLang() + "], the new namespace for stored scripts will only use (id) instead of (lang, id)");
            }

            return this;
        }

        /**
         * Delete a script from the existing stored scripts.  The script will be removed from the
         * new namespace if the script language matches the current script under the same id or
         * if the script language is {@code null}.  The script will be removed from the deprecated
         * namespace on any delete either using using the specified lang parameter or the language
         * found from looking up the script in the new namespace.
         * <p>
         * Take for example a scripts {@link Map} with {"A" -- D1, "A#L1" -- D1, "A#L0" -- D0}.
         * If a script is removed specified by an id 'A' and lang {@code null} then the scripts
         * {@link Map} will be {"A#L0" -- D0}.  To remove the final script, the deprecated
         * namespace must be used, so an id 'A' and lang 'L0' would need to be specified.
         * @param id The user-specified id to use for the look up.
         * @param lang The user-specified language to use for the look up if using the deprecated
         *             namespace, otherwise {@code null}.
         */
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
    static ScriptMetaData deleteStoredScript(ScriptMetaData previous, String id, String lang) {
        Builder builder = new ScriptMetaData.Builder(previous);
        builder.deleteScript(id, lang);

        return builder.build();
    }

    /**
     * Standard logger necessary for allocation of the deprecation logger.
     */
    private static final Logger LOGGER = ESLoggerFactory.getLogger(ScriptMetaData.class);

    /**
     * Deprecation logger necessary for namespace changes related to stored scripts.
     */
    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(LOGGER);

    /**
     * The type of {@link ClusterState} data.
     */
    public static final String TYPE = "stored_scripts";

    /**
     * This will parse XContent into {@link ScriptMetaData}.
     *
     * The following format will be parsed for the new namespace:
     *
     * {@code
     * {
     *     "<id>" : "<{@link StoredScriptSource#fromXContent(XContentParser)}>",
     *     "<id>" : "<{@link StoredScriptSource#fromXContent(XContentParser)}>",
     *     ...
     * }
     * }
     *
     * The following format will be parsed for the deprecated namespace:
     *
     * {@code
     * {
     *     "<id>" : "<code>",
     *     "<id>" : "<code>",
     *     ...
     * }
     * }
     *
     * Note when using the deprecated namespace, the language will be pulled from
     * the id and options will be set to an empty {@link Map}.
     */
    public static ScriptMetaData fromXContent(XContentParser parser) throws IOException {
        Map<String, StoredScriptSource> scripts = new HashMap<>();
        String id = null;
        StoredScriptSource source;

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

                    if (split == -1) {
                        throw new IllegalArgumentException("illegal stored script id [" + id + "], does not contain lang");
                    } else {
                        source = new StoredScriptSource(id.substring(0, split), parser.text(), Collections.emptyMap());
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
            if (in.getVersion().before(Version.V_5_3_0_UNRELEASED)) {
                int split = id.indexOf('#');

                if (split == -1) {
                    throw new IllegalArgumentException("illegal stored script id [" + id + "], does not contain lang");
                } else {
                    source = new StoredScriptSource(in);
                    source = new StoredScriptSource(id.substring(0, split), source.getCode(), Collections.emptyMap());
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
        if (out.getVersion().onOrAfter(Version.V_5_3_0_UNRELEASED)) {
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
     * Retrieves a stored script from the new namespace if lang is {@code null}.
     * Otherwise, returns a stored script from the deprecated namespace.  Either
     * way an id is required.
     */
    StoredScriptSource getStoredScript(String id, String lang) {
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
