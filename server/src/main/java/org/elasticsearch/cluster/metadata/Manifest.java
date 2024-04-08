/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.gateway.MetadataStateFormat;
import org.elasticsearch.index.Index;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class represents the manifest file, which is the entry point for reading meta data from disk.
 * Metadata consists of global metadata and index metadata.
 * When new version of metadata is written it's assigned some generation long value.
 * Global metadata generation could be obtained by calling {@link #globalGeneration()}.
 * Index metadata generation could be obtained by calling {@link #indexGenerations()}.
 */
public record Manifest(long currentTerm, long clusterStateVersion, long globalGeneration, Map<Index, Long> indexGenerations)
    implements
        ToXContentFragment {

    private static final long MISSING_GLOBAL_GENERATION = -1L;
    private static final long MISSING_CURRENT_TERM = 0L;
    private static final long UNKNOWN_CURRENT_TERM = MISSING_CURRENT_TERM;
    private static final long MISSING_CLUSTER_STATE_VERSION = 0L;
    private static final long UNKNOWN_CLUSTER_STATE_VERSION = MISSING_CLUSTER_STATE_VERSION;

    public static Manifest unknownCurrentTermAndVersion(long globalGeneration, Map<Index, Long> indexGenerations) {
        return new Manifest(UNKNOWN_CURRENT_TERM, UNKNOWN_CLUSTER_STATE_VERSION, globalGeneration, indexGenerations);
    }

    private static final String MANIFEST_FILE_PREFIX = "manifest-";
    private static final ToXContent.Params MANIFEST_FORMAT_PARAMS = new ToXContent.MapParams(Collections.singletonMap("binary", "true"));

    public static final MetadataStateFormat<Manifest> FORMAT = new MetadataStateFormat<Manifest>(MANIFEST_FILE_PREFIX) {

        @Override
        public void toXContent(XContentBuilder builder, Manifest state) throws IOException {
            state.toXContent(builder, MANIFEST_FORMAT_PARAMS);
        }

        @Override
        public Manifest fromXContent(XContentParser parser) throws IOException {
            return Manifest.fromXContent(parser);
        }
    };

    /*
     * Code below this comment is for XContent parsing/generation
     */

    private static final ParseField CURRENT_TERM_PARSE_FIELD = new ParseField("current_term");
    private static final ParseField CLUSTER_STATE_VERSION_PARSE_FIELD = new ParseField("cluster_state_version");
    private static final ParseField GENERATION_PARSE_FIELD = new ParseField("generation");
    private static final ParseField INDEX_GENERATIONS_PARSE_FIELD = new ParseField("index_generations");

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(CURRENT_TERM_PARSE_FIELD.getPreferredName(), currentTerm);
        builder.field(CLUSTER_STATE_VERSION_PARSE_FIELD.getPreferredName(), clusterStateVersion);
        builder.field(GENERATION_PARSE_FIELD.getPreferredName(), globalGeneration);
        builder.xContentList(INDEX_GENERATIONS_PARSE_FIELD.getPreferredName(), indexEntryList());
        return builder;
    }

    private static long requireNonNullElseDefault(Long value, long defaultValue) {
        return value != null ? value : defaultValue;
    }

    private List<IndexEntry> indexEntryList() {
        return indexGenerations.entrySet().stream().map(entry -> new IndexEntry(entry.getKey(), entry.getValue())).toList();
    }

    private static long currentTerm(Object[] manifestFields) {
        return requireNonNullElseDefault((Long) manifestFields[0], MISSING_CURRENT_TERM);
    }

    private static long clusterStateVersion(Object[] manifestFields) {
        return requireNonNullElseDefault((Long) manifestFields[1], MISSING_CLUSTER_STATE_VERSION);
    }

    private static long generation(Object[] manifestFields) {
        return requireNonNullElseDefault((Long) manifestFields[2], MISSING_GLOBAL_GENERATION);
    }

    @SuppressWarnings("unchecked")
    private static Map<Index, Long> indices(Object[] manifestFields) {
        List<IndexEntry> listOfIndices = (List<IndexEntry>) manifestFields[3];
        return listOfIndices.stream().collect(Collectors.toMap(IndexEntry::index, IndexEntry::generation));
    }

    private static final ConstructingObjectParser<Manifest, Void> PARSER = new ConstructingObjectParser<>(
        "manifest",
        manifestFields -> new Manifest(
            currentTerm(manifestFields),
            clusterStateVersion(manifestFields),
            generation(manifestFields),
            indices(manifestFields)
        )
    );

    static {
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), CURRENT_TERM_PARSE_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), CLUSTER_STATE_VERSION_PARSE_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), GENERATION_PARSE_FIELD);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), IndexEntry.INDEX_ENTRY_PARSER, INDEX_GENERATIONS_PARSE_FIELD);
    }

    public static Manifest fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public boolean isEmpty() {
        return currentTerm == MISSING_CURRENT_TERM
            && clusterStateVersion == MISSING_CLUSTER_STATE_VERSION
            && globalGeneration == MISSING_GLOBAL_GENERATION
            && indexGenerations.isEmpty();
    }

    public static Manifest empty() {
        return new Manifest(MISSING_CURRENT_TERM, MISSING_CLUSTER_STATE_VERSION, MISSING_GLOBAL_GENERATION, Collections.emptyMap());
    }

    public boolean isGlobalGenerationMissing() {
        return globalGeneration == MISSING_GLOBAL_GENERATION;
    }

    private record IndexEntry(Index index, long generation) implements ToXContentFragment {
        private static final ParseField INDEX_GENERATION_PARSE_FIELD = new ParseField("generation");
        private static final ParseField INDEX_PARSE_FIELD = new ParseField("index");

        static final ConstructingObjectParser<IndexEntry, Void> INDEX_ENTRY_PARSER = new ConstructingObjectParser<>(
            "indexEntry",
            indexAndGeneration -> new IndexEntry((Index) indexAndGeneration[0], (long) indexAndGeneration[1])
        );

        static {
            INDEX_ENTRY_PARSER.declareField(
                ConstructingObjectParser.constructorArg(),
                Index::fromXContent,
                INDEX_PARSE_FIELD,
                ObjectParser.ValueType.OBJECT
            );
            INDEX_ENTRY_PARSER.declareLong(ConstructingObjectParser.constructorArg(), INDEX_GENERATION_PARSE_FIELD);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(INDEX_PARSE_FIELD.getPreferredName(), index);
            builder.field(GENERATION_PARSE_FIELD.getPreferredName(), generation);
            builder.endObject();
            return builder;
        }
    }
}
