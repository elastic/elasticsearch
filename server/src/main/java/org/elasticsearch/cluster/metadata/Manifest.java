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
    private static final long MISSING_GLOBAL_GENERATION = -1;

    private final long globalGeneration;
    private final Map<Index, Long> indexGenerations;

    public Manifest(long globalGeneration, Map<Index, Long> indexGenerations) {
        this.globalGeneration = globalGeneration;
        this.indexGenerations = indexGenerations;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Manifest manifest = (Manifest) o;
        return globalGeneration == manifest.globalGeneration &&
                Objects.equals(indexGenerations, manifest.indexGenerations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(globalGeneration, indexGenerations);
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

    private static final ParseField GENERATION_PARSE_FIELD = new ParseField("generation");
    private static final ParseField INDEX_GENERATIONS_PARSE_FIELD = new ParseField("index_generations");

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(GENERATION_PARSE_FIELD.getPreferredName(), globalGeneration);
        builder.array(INDEX_GENERATIONS_PARSE_FIELD.getPreferredName(), indexEntryList().toArray());
        return builder;
    }

    private List<IndexEntry> indexEntryList() {
        return indexGenerations.entrySet().stream().
                map(entry -> new IndexEntry(entry.getKey(), entry.getValue())).
                collect(Collectors.toList());
    }

    private static long generation(Object[] generationAndListOfIndexEntries) {
        return (Long) generationAndListOfIndexEntries[0];
    }

    private static Map<Index, Long> indices(Object[] generationAndListOfIndexEntries) {
        List<IndexEntry> listOfIndices = (List<IndexEntry>) generationAndListOfIndexEntries[1];
        return listOfIndices.stream().collect(Collectors.toMap(IndexEntry::getIndex, IndexEntry::getGeneration));
    }

    private static final ConstructingObjectParser<Manifest, Void> PARSER = new ConstructingObjectParser<>(
            "manifest",
            generationAndListOfIndexEntries ->
                    new Manifest(generation(generationAndListOfIndexEntries), indices(generationAndListOfIndexEntries)));

    static {
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), GENERATION_PARSE_FIELD);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), IndexEntry.INDEX_ENTRY_PARSER, INDEX_GENERATIONS_PARSE_FIELD);
    }

    public static Manifest fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public boolean isEmpty() {
        return globalGeneration == MISSING_GLOBAL_GENERATION && indexGenerations.isEmpty();
    }

    public static Manifest empty() {
        return new Manifest(MISSING_GLOBAL_GENERATION, Collections.emptyMap());
    }

    public boolean isGlobalGenerationMissing() {
        return globalGeneration == MISSING_GLOBAL_GENERATION;
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

