/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.script;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link ScriptMetadata} is used to store user-defined scripts
 * as part of the {@link ClusterState} using only an id as the key.
 */
public final class ScriptMetadata implements Metadata.Custom, Writeable, ToXContentFragment {

    /**
     * Standard logger used to warn about dropped scripts.
     */
    private static final Logger logger = LogManager.getLogger(ScriptMetadata.class);

    /**
     * A builder used to modify the currently stored scripts data held within
     * the {@link ClusterState}.  Scripts can be added or deleted, then built
     * to generate a new {@link Map} of scripts that will be used to update
     * the current {@link ClusterState}.
     */
    public static final class Builder {

        private final Map<String, StoredScriptSource> scripts;

        /**
         * @param previous The current {@link ScriptMetadata} or {@code null} if there
         *                 is no existing {@link ScriptMetadata}.
         */
        public Builder(ScriptMetadata previous) {
            this.scripts = previous == null ? new HashMap<>() : new HashMap<>(previous.scripts);
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
         * @return A {@link ScriptMetadata} with the updated {@link Map} of scripts.
         */
        public ScriptMetadata build() {
            return new ScriptMetadata(scripts);
        }
    }

    static final class ScriptMetadataDiff implements NamedDiff<Metadata.Custom> {

        final Diff<Map<String, StoredScriptSource>> pipelines;

        ScriptMetadataDiff(ScriptMetadata before, ScriptMetadata after) {
            this.pipelines = DiffableUtils.diff(before.scripts, after.scripts, DiffableUtils.getStringKeySerializer());
        }

        ScriptMetadataDiff(StreamInput in) throws IOException {
            pipelines = DiffableUtils.readJdkMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                StoredScriptSource::new,
                StoredScriptSource::readDiffFrom
            );
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            return new ScriptMetadata(pipelines.apply(((ScriptMetadata) part).scripts));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            pipelines.writeTo(out);
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT.minimumCompatibilityVersion();
        }
    }

    /**
     * Convenience method to build and return a new
     * {@link ScriptMetadata} adding the specified stored script.
     */
    static ScriptMetadata putStoredScript(ScriptMetadata previous, String id, StoredScriptSource source) {
        Builder builder = new Builder(previous);
        builder.storeScript(id, source);

        return builder.build();
    }

    /**
     * Convenience method to build and return a new
     * {@link ScriptMetadata} deleting the specified stored script.
     */
    static ScriptMetadata deleteStoredScript(ScriptMetadata previous, String id) {
        Builder builder = new ScriptMetadata.Builder(previous);
        builder.deleteScript(id);

        return builder.build();
    }

    /**
     * The type of {@link ClusterState} data.
     */
    public static final String TYPE = "stored_scripts";

    /**
     * This will parse XContent into {@link ScriptMetadata}.
     *
     * The following format will be parsed:
     *
     * {@code
     * {
     *     "<id>" : "<{@link StoredScriptSource#fromXContent(XContentParser, boolean)}>",
     *     "<id>" : "<{@link StoredScriptSource#fromXContent(XContentParser, boolean)}>",
     *     ...
     * }
     * }
     */
    public static ScriptMetadata fromXContent(XContentParser parser) throws IOException {
        Map<String, StoredScriptSource> scripts = new HashMap<>();
        String id = null;

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
                case FIELD_NAME -> id = parser.currentName();
                case START_OBJECT -> {
                    if (id == null) {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "unexpected token [" + token + "], expected [<id>, <code>, {]"
                        );
                    }
                    StoredScriptSource source = StoredScriptSource.fromXContent(parser, true);
                    // as of 8.0 we drop scripts/templates with an empty source
                    // this check should be removed for the next upgradable version after 8.0
                    // since there is a guarantee no more empty scripts will exist
                    if (source.getSource().isEmpty()) {
                        if (Script.DEFAULT_TEMPLATE_LANG.equals(source.getLang())) {
                            logger.warn("empty template [" + id + "] found and dropped");
                        } else {
                            logger.warn("empty script [" + id + "] found and dropped");
                        }
                    } else {
                        scripts.put(id, source);
                    }
                    id = null;
                }
                default -> throw new ParsingException(
                    parser.getTokenLocation(),
                    "unexpected token [" + token + "], expected [<id>, <code>, {]"
                );
            }

            token = parser.nextToken();
        }

        return new ScriptMetadata(scripts);
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return new ScriptMetadataDiff(in);
    }

    private final Map<String, StoredScriptSource> scripts;

    /**
     * Standard constructor to create metadata to store scripts.
     * @param scripts The currently stored scripts.  Must not be {@code null},
     *                use and empty {@link Map} to specify there were no
     *                previously stored scripts.
     */
    ScriptMetadata(Map<String, StoredScriptSource> scripts) {
        this.scripts = Collections.unmodifiableMap(scripts);
    }

    public ScriptMetadata(StreamInput in) throws IOException {
        Map<String, StoredScriptSource> scripts = new HashMap<>();
        StoredScriptSource source;
        int size = in.readVInt();

        for (int i = 0; i < size; i++) {
            String id = in.readString();
            source = new StoredScriptSource(in);
            scripts.put(id, source);
        }

        this.scripts = Collections.unmodifiableMap(scripts);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(scripts.size());

        for (Map.Entry<String, StoredScriptSource> entry : scripts.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeTo(out);
        }
    }

    /**
     * This will write XContent from {@link ScriptMetadata}.  The following format will be written:
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
    public Diff<Metadata.Custom> diff(Metadata.Custom before) {
        return new ScriptMetadataDiff((ScriptMetadata) before, this);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT.minimumCompatibilityVersion();
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    /**
     * Returns the map of stored scripts.
     */
    Map<String, StoredScriptSource> getStoredScripts() {
        return scripts;
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

        ScriptMetadata that = (ScriptMetadata) o;

        return scripts.equals(that.scripts);

    }

    @Override
    public int hashCode() {
        return scripts.hashCode();
    }

    @Override
    public String toString() {
        return "ScriptMetadata{" + "scripts=" + scripts + '}';
    }
}
