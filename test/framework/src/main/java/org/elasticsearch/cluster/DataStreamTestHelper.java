/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.cluster;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.DataStream.getDefaultBackingIndexName;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;

public final class DataStreamTestHelper {

    private static final Settings.Builder SETTINGS = ESTestCase.settings(Version.CURRENT).put("index.hidden", true);
    private static final int NUMBER_OF_SHARDS = 1;
    private static final int NUMBER_OF_REPLICAS = 1;

    public static IndexMetadata.Builder createFirstBackingIndex(String dataStreamName) {
        return createBackingIndex(dataStreamName, 1);
    }

    public static IndexMetadata.Builder createBackingIndex(String dataStreamName, int generation) {
        return IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, generation))
            .settings(SETTINGS)
            .numberOfShards(NUMBER_OF_SHARDS)
            .numberOfReplicas(NUMBER_OF_REPLICAS);
    }

    public static IndexMetadata.Builder getIndexMetadataBuilderForIndex(Index index) {
        return IndexMetadata.builder(index.getName())
            .settings(Settings.builder().put(SETTINGS.build()).put(SETTING_INDEX_UUID, index.getUUID()))
            .numberOfShards(NUMBER_OF_SHARDS)
            .numberOfReplicas(NUMBER_OF_REPLICAS);
    }

    public static DataStream.TimestampField createTimestampField(String fieldName) {
        return new DataStream.TimestampField(fieldName);
    }

    public static String generateMapping(String timestampFieldName) {
        return "{\n" +
            "      \"properties\": {\n" +
            "        \"" + timestampFieldName + "\": {\n" +
            "          \"type\": \"date\"\n" +
            "        }\n" +
            "      }\n" +
            "    }";
    }

    public static String generateMapping(String timestampFieldName, String type) {
        return "{\n" +
            "      \"_data_stream_timestamp\": {\n" +
            "        \"enabled\": true\n" +
            "      }," +
            "      \"properties\": {\n" +
            "        \"" + timestampFieldName + "\": {\n" +
            "          \"type\": \"" + type + "\"\n" +
            "        }\n" +
            "      }\n" +
            "    }";
    }

    public static List<Index> randomIndexInstances() {
        int numIndices = ESTestCase.randomIntBetween(0, 128);
        List<Index> indices = new ArrayList<>(numIndices);
        for (int i = 0; i < numIndices; i++) {
            indices.add(new Index(randomAlphaOfLength(10).toLowerCase(Locale.ROOT), UUIDs.randomBase64UUID(LuceneTestCase.random())));
        }
        return indices;
    }

    public static DataStream randomInstance() {
        List<Index> indices = randomIndexInstances();
        long generation = indices.size() + ESTestCase.randomLongBetween(1, 128);
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        indices.add(new Index(getDefaultBackingIndexName(dataStreamName, generation), UUIDs.randomBase64UUID(LuceneTestCase.random())));
        return new DataStream(dataStreamName, createTimestampField("@timestamp"), indices, generation);
    }

    /**
     * Constructs {@code ClusterState} with the specified data streams and indices.
     *
     * @param dataStreams The names of the data streams to create with their respective number of backing indices
     * @param indexNames  The names of indices to create that do not back any data streams
     */
    public static ClusterState getClusterStateWithDataStreams(List<Tuple<String, Integer>> dataStreams, List<String> indexNames) {
        Metadata.Builder builder = Metadata.builder();

        List<IndexMetadata> allIndices = new ArrayList<>();
        for (Tuple<String, Integer> dsTuple : dataStreams) {
            List<IndexMetadata> backingIndices = new ArrayList<>();
            for (int backingIndexNumber = 1; backingIndexNumber <= dsTuple.v2(); backingIndexNumber++) {
                backingIndices.add(createIndexMetadata(getDefaultBackingIndexName(dsTuple.v1(), backingIndexNumber), true));
            }
            allIndices.addAll(backingIndices);

            DataStream ds = new DataStream(
                dsTuple.v1(),
                createTimestampField("@timestamp"),
                backingIndices.stream().map(IndexMetadata::getIndex).collect(Collectors.toList()),
                dsTuple.v2()
            );
            builder.put(ds);
        }

        for (String indexName : indexNames) {
            allIndices.add(createIndexMetadata(indexName, false));
        }

        for (IndexMetadata index : allIndices) {
            builder.put(index, false);
        }

        return ClusterState.builder(new ClusterName("_name")).metadata(builder).build();
    }

    private static IndexMetadata createIndexMetadata(String name, boolean hidden) {
        Settings.Builder b = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).put("index.hidden", hidden);

        return IndexMetadata.builder(name).settings(b).numberOfShards(1).numberOfReplicas(1).build();
    }
}
