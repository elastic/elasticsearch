/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SearchEngineMetadata implements Metadata.Custom {

    public static final String TYPE = "content_indices";
    public static final ParseField CONTENT_INDICES = new ParseField("content_indices");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<SearchEngineMetadata, Void> PARSER = new ConstructingObjectParser<>(TYPE, false, args -> {
        Map<String, SearchEngine> searchEngines = (Map<String, SearchEngine>) args[0];
        return new SearchEngineMetadata(searchEngines);
    });

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {
            Map<String, SearchEngine> contentIndices = new HashMap<>();
            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                String name = p.currentName();
                contentIndices.put(name, SearchEngine.fromXContent(p));
            }
            return contentIndices;
        }, CONTENT_INDICES);
    }

    private final Map<String, SearchEngine> searchEngines;

    public SearchEngineMetadata(Map<String, SearchEngine> searchEngines) {
        this.searchEngines = searchEngines;
    }

    public SearchEngineMetadata(StreamInput in) throws IOException {
        this(in.readMap(StreamInput::readString, SearchEngine::new));
    }

    public Map<String, SearchEngine> searchEngines() {
        return searchEngines;
    }

    @Override
    public Diff<Metadata.Custom> diff(Metadata.Custom before) {
        return new SearchEngineMetadata.SearchEngineMetadataDiff((SearchEngineMetadata) before, this);
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return new SearchEngineMetadataDiff(in);
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_8_3_0;
    }

    public static SearchEngineMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.xContentValuesMap(CONTENT_INDICES.getPreferredName(), searchEngines);
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(this.searchEngines, StreamOutput::writeString, (stream, val) -> val.writeTo(stream));
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.searchEngines);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        SearchEngineMetadata other = (SearchEngineMetadata) obj;

        return Objects.equals(this.searchEngines, other.searchEngines);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    static class SearchEngineMetadataDiff implements NamedDiff<Metadata.Custom> {

        final Diff<Map<String, SearchEngine>> searchEnginesDiff;

        SearchEngineMetadataDiff(SearchEngineMetadata before, SearchEngineMetadata after) {
            this.searchEnginesDiff = DiffableUtils.diff(
                before.searchEngines,
                after.searchEngines,
                DiffableUtils.getStringKeySerializer()
            );
        }

        SearchEngineMetadataDiff(StreamInput in) throws IOException {
            this.searchEnginesDiff = DiffableUtils.readJdkMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                SearchEngine::new,
                SearchEngine::readDiffFrom
            );
        }

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            return new SearchEngineMetadata(
                searchEnginesDiff.apply(((SearchEngineMetadata) part).searchEngines)
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            searchEnginesDiff.writeTo(out);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.V_8_6_0;
        }
    }
}
