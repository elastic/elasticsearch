/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.metadata;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.DataStream.getDefaultBackingIndexName;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.elasticsearch.test.ESTestCase.generateRandomStringArray;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomMap;

public final class DataStreamTestHelper {

    private static final Settings.Builder SETTINGS = ESTestCase.settings(Version.CURRENT).put("index.hidden", true);
    private static final int NUMBER_OF_SHARDS = 1;
    private static final int NUMBER_OF_REPLICAS = 1;

    public static IndexMetadata.Builder createFirstBackingIndex(String dataStreamName) {
        return createBackingIndex(dataStreamName, 1, System.currentTimeMillis());
    }

    public static IndexMetadata.Builder createFirstBackingIndex(String dataStreamName, long epochMillis) {
        return createBackingIndex(dataStreamName, 1, epochMillis);
    }

    public static IndexMetadata.Builder createBackingIndex(String dataStreamName, int generation) {
        return createBackingIndex(dataStreamName, generation, System.currentTimeMillis());
    }

    public static IndexMetadata.Builder createBackingIndex(String dataStreamName, int generation, long epochMillis) {
        return IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, generation, epochMillis))
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
        return randomInstance(System::currentTimeMillis);
    }

    public static DataStream randomInstance(String name) {
        return randomInstance(name, System::currentTimeMillis);
    }

    public static DataStream randomInstance(LongSupplier timeProvider) {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        return randomInstance(dataStreamName, timeProvider);
    }

    public static DataStream randomInstance(String dataStreamName, LongSupplier timeProvider) {
        List<Index> indices = randomIndexInstances();
        long generation = indices.size() + ESTestCase.randomLongBetween(1, 128);
        indices.add(new Index(getDefaultBackingIndexName(dataStreamName, generation), UUIDs.randomBase64UUID(LuceneTestCase.random())));
        Map<String, Object> metadata = null;
        if (randomBoolean()) {
            metadata = new HashMap<>();
            metadata.put("key", "value");
        }
        return new DataStream(dataStreamName, createTimestampField("@timestamp"), indices, generation, metadata,
            randomBoolean(), randomBoolean(), false, timeProvider);
    }

    public static DataStreamAlias randomAliasInstance() {
        List<String> dataStreams = Arrays.asList(generateRandomStringArray(5, 5, false, false));
        return new DataStreamAlias(
            randomAlphaOfLength(5),
            dataStreams,
            randomBoolean() ? randomFrom(dataStreams) : null,
            randomBoolean() ? randomMap(1, 4, () -> new Tuple<>("term", Collections.singletonMap("year", "2022"))) : null);
    }

    /**
     * Constructs {@code ClusterState} with the specified data streams and indices.
     *
     * @param dataStreams The names of the data streams to create with their respective number of backing indices
     * @param indexNames  The names of indices to create that do not back any data streams
     */
    public static ClusterState getClusterStateWithDataStreams(List<Tuple<String, Integer>> dataStreams, List<String> indexNames) {
        return getClusterStateWithDataStreams(dataStreams, indexNames, 1);
    }

    /**
     * Constructs {@code ClusterState} with the specified data streams and indices.
     *
     * @param dataStreams The names of the data streams to create with their respective number of backing indices
     * @param indexNames  The names of indices to create that do not back any data streams
     * @param replicas number of replicas
     */
    public static ClusterState getClusterStateWithDataStreams(List<Tuple<String, Integer>> dataStreams, List<String> indexNames,
                                                              int replicas) {
        Metadata.Builder builder = Metadata.builder();

        List<IndexMetadata> allIndices = new ArrayList<>();
        for (Tuple<String, Integer> dsTuple : dataStreams) {
            List<IndexMetadata> backingIndices = new ArrayList<>();
            for (int backingIndexNumber = 1; backingIndexNumber <= dsTuple.v2(); backingIndexNumber++) {
                backingIndices.add(createIndexMetadata(getDefaultBackingIndexName(dsTuple.v1(), backingIndexNumber), true, replicas));
            }
            allIndices.addAll(backingIndices);

            DataStream ds = new DataStream(
                dsTuple.v1(),
                createTimestampField("@timestamp"),
                backingIndices.stream().map(IndexMetadata::getIndex).collect(Collectors.toList()),
                dsTuple.v2(),
                null
            );
            builder.put(ds);
        }

        for (String indexName : indexNames) {
            allIndices.add(createIndexMetadata(indexName, false, replicas));
        }

        for (IndexMetadata index : allIndices) {
            builder.put(index, false);
        }

        return ClusterState.builder(new ClusterName("_name")).metadata(builder).build();
    }

    private static IndexMetadata createIndexMetadata(String name, boolean hidden, int replicas) {
        Settings.Builder b = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).put("index.hidden", hidden);

        return IndexMetadata.builder(name).settings(b).numberOfShards(1).numberOfReplicas(replicas).build();
    }

    public static String backingIndexPattern(String dataStreamName, long generation) {
        return String.format(Locale.ROOT, "\\.ds-%s-(\\d{4}\\.\\d{2}\\.\\d{2}-)?%06d",dataStreamName, generation);
    }

    public static Matcher<String> backingIndexEqualTo(String dataStreamName, int generation) {
        return new TypeSafeMatcher<String>() {

            @Override
            protected boolean matchesSafely(String backingIndexName) {
                if (backingIndexName == null) {
                    return false;
                }

                int indexOfLastDash = backingIndexName.lastIndexOf('-');
                String actualDataStreamName = parseDataStreamName(backingIndexName, indexOfLastDash);
                int actualGeneration = parseGeneration(backingIndexName, indexOfLastDash);
                return actualDataStreamName.equals(dataStreamName) && actualGeneration == generation;
            }

            @Override
            protected void describeMismatchSafely(String backingIndexName, Description mismatchDescription) {
                int indexOfLastDash = backingIndexName.lastIndexOf('-');
                String dataStreamName = parseDataStreamName(backingIndexName, indexOfLastDash);
                int generation = parseGeneration(backingIndexName, indexOfLastDash);
                mismatchDescription.appendText(" was data stream name ").appendValue(dataStreamName)
                    .appendText(" and generation ").appendValue(generation);
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("expected data stream name ").appendValue(dataStreamName)
                    .appendText(" and expected generation ").appendValue(generation);
            }

            private String parseDataStreamName(String backingIndexName, int indexOfLastDash) {
                return backingIndexName.substring(4, backingIndexName.lastIndexOf('-', indexOfLastDash - 1));
            }

            private int parseGeneration(String backingIndexName, int indexOfLastDash) {
                return Integer.parseInt(backingIndexName.substring(indexOfLastDash + 1));
            }
        };
    }

}
