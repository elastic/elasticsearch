/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.gateway.MetaDataStateFormat;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This class represents the manifest file, which is the entry point for reading meta data from disk.
 * Metadata consists of global metadata and index metadata.
 * When new version of metadata is written it's assigned some generation long value.
 * Global metadata generation could be obtained by calling {@link #getGlobalGeneration()}.
 * Index metadata generation could be obtained by calling {@link #getIndexGenerations()}.
 */
public class Manifest implements ToXContentFragment {
    private static final long MISSING = -1;

    private final long globalGeneration;
    private final Map<Index, Long> indexGenerations;
    private final long currentTerm;
    private final long clusterStateVersion;

    public Manifest(long currentTerm, long clusterStateVersion, long globalGeneration, Map<Index, Long> indexGenerations) {
        this.currentTerm = currentTerm;
        this.clusterStateVersion = clusterStateVersion;
        this.globalGeneration = globalGeneration;
        this.indexGenerations = indexGenerations;
    }

    public Manifest(long globalGeneration, Map<Index, Long> indexGenerations) {
        this(MISSING, MISSING, globalGeneration, indexGenerations);
    }

    /**
     * Returns global metadata generation.
     */
    public long getGlobalGeneration() {
        return globalGeneration;
    }

    /**
     * Returns map from {@link Index} to index metadata generation.
     */
    public Map<Index, Long> getIndexGenerations() {
        return indexGenerations;
    }

    public long getCurrentTerm() {
        return currentTerm;
    }

    public long getClusterStateVersion() {
        return clusterStateVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Manifest manifest = (Manifest) o;
        return currentTerm == manifest.currentTerm &&
               clusterStateVersion == manifest.clusterStateVersion &&
               globalGeneration == manifest.globalGeneration &&
               Objects.equals(indexGenerations, manifest.indexGenerations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(currentTerm, clusterStateVersion, globalGeneration, indexGenerations);
    }

    @Override
    public String toString() {
        return "Manifest{" +
                "currentTerm=" + currentTerm +
                ", clusterStateVersion=" + clusterStateVersion +
                ", globalGeneration=" + globalGeneration +
                ", indexGenerations=" + indexGenerations +
                '}';
    }

    private static final String MANIFEST_FILE_PREFIX = "manifest-";
    private static final ToXContent.Params MANIFEST_FORMAT_PARAMS = new ToXContent.MapParams(Collections.singletonMap("binary", "true"));

    public static final MetaDataStateFormat<Manifest> FORMAT = new MetaDataStateFormat<Manifest>(MANIFEST_FILE_PREFIX) {

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
        builder.array(INDEX_GENERATIONS_PARSE_FIELD.getPreferredName(), indexEntryList().toArray());
        return builder;
    }

    private static long requireNonNullElseMissing(Long value) {
        return value != null ? value : MISSING;
    }

    private List<IndexEntry> indexEntryList() {
        return indexGenerations.entrySet().stream().
                map(entry -> new IndexEntry(entry.getKey(), entry.getValue())).
                collect(Collectors.toList());
    }

    private static long currentTerm(Object[] manifestFields) {
        return requireNonNullElseMissing((Long) manifestFields[0]);
    }

    private static long clusterStateVersion(Object[] manifestFields) {
        return requireNonNullElseMissing((Long) manifestFields[1]);
    }

    private static long generation(Object[] manifestFields) {
        return requireNonNullElseMissing((Long) manifestFields[2]);
    }

    private static Map<Index, Long> indices(Object[] manifestFields) {
        List<IndexEntry> listOfIndices = (List<IndexEntry>) manifestFields[3];
        return listOfIndices.stream().collect(Collectors.toMap(IndexEntry::getIndex, IndexEntry::getGeneration));
    }

    private static final ConstructingObjectParser<Manifest, Void> PARSER = new ConstructingObjectParser<>(
            "manifest",
            manifestFields ->
                    new Manifest(currentTerm(manifestFields), clusterStateVersion(manifestFields), generation(manifestFields),
                            indices(manifestFields)));

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
        return currentTerm == MISSING && clusterStateVersion == MISSING && globalGeneration == MISSING && indexGenerations.isEmpty();
    }

    public static Manifest empty() {
        return new Manifest(MISSING, MISSING, MISSING, Collections.emptyMap());
    }

    public boolean isGlobalGenerationMissing() {
        return globalGeneration == MISSING;
    }

    private static final class IndexEntry implements ToXContentFragment {
        private static final ParseField INDEX_GENERATION_PARSE_FIELD = new ParseField("generation");
        private static final ParseField INDEX_PARSE_FIELD = new ParseField("index");

        static final ConstructingObjectParser<IndexEntry, Void> INDEX_ENTRY_PARSER = new ConstructingObjectParser<>(
                "indexEntry",
                indexAndGeneration -> new IndexEntry((Index) indexAndGeneration[0], (long) indexAndGeneration[1]));

        static {
            INDEX_ENTRY_PARSER.declareField(ConstructingObjectParser.constructorArg(),
                    Index::fromXContent, INDEX_PARSE_FIELD, ObjectParser.ValueType.OBJECT);
            INDEX_ENTRY_PARSER.declareLong(ConstructingObjectParser.constructorArg(), INDEX_GENERATION_PARSE_FIELD);
        }

        private final long generation;
        private final Index index;

        IndexEntry(Index index, long generation) {
            this.index = index;
            this.generation = generation;
        }

        public long getGeneration() {
            return generation;
        }

        public Index getIndex() {
            return index;
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

