/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.Script;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.VersionUtils;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class MappingStatsTests extends AbstractWireSerializingTestCase<MappingStats> {

    private static final Settings SINGLE_SHARD_NO_REPLICAS = Settings.builder()
        .put("index.number_of_replicas", 0)
        .put("index.number_of_shards", 1)
        .put("index.version.created", Version.CURRENT)
        .build();

    public static final String MAPPING_TEMPLATE = """
        {
            "runtime": {
                "keyword1": {
                    "type": "keyword",
                    "script": %s
                },
                "keyword2": {
                    "type": "keyword"
                },
                "object.keyword3": {
                    "type": "keyword",
                    "script": %s
                },
                "long": {
                    "type": "long",
                    "script": %s
                },
                "long2": {
                    "type": "long",
                    "script": %s
                }
            },
            "properties": {
                "object": {
                    "type": "object",
                    "properties": {
                        "keyword3": {
                            "type": "keyword"
                        }
                    }
                },
                "long3": {
                    "type": "long",
                    "script": %s
                },
                "long4": {
                    "type": "long",
                    "script": %s
                },
                "keyword3": {
                    "type": "keyword",
                    "script": %s
                }
            }
        }""";

    private static final String SCRIPT_1 = scriptAsJSON("doc['field'] + doc.field + params._source.field");
    private static final String SCRIPT_2 = scriptAsJSON("doc['field']");
    private static final String SCRIPT_3 = scriptAsJSON("params._source.field + params._source.field \n + params._source.field");
    private static final String SCRIPT_4 = scriptAsJSON("params._source.field");

    public void testToXContent() {
        String mapping = MAPPING_TEMPLATE.formatted(SCRIPT_1, SCRIPT_2, SCRIPT_3, SCRIPT_4, SCRIPT_3, SCRIPT_4, SCRIPT_1);
        IndexMetadata meta = IndexMetadata.builder("index").settings(SINGLE_SHARD_NO_REPLICAS).putMapping(mapping).build();
        IndexMetadata meta2 = IndexMetadata.builder("index2").settings(SINGLE_SHARD_NO_REPLICAS).putMapping(mapping).build();
        Metadata metadata = Metadata.builder().put(meta, false).put(meta2, false).build();
        assertThat(metadata.getMappingsByHash(), Matchers.aMapWithSize(1));
        MappingStats mappingStats = MappingStats.of(metadata, () -> {});
        assertEquals("""
            {
              "mappings" : {
                "field_types" : [
                  {
                    "name" : "keyword",
                    "count" : 4,
                    "index_count" : 2,
                    "script_count" : 2,
                    "lang" : [
                      "painless"
                    ],
                    "lines_max" : 1,
                    "lines_total" : 2,
                    "chars_max" : 47,
                    "chars_total" : 94,
                    "source_max" : 1,
                    "source_total" : 2,
                    "doc_max" : 2,
                    "doc_total" : 4
                  },
                  {
                    "name" : "long",
                    "count" : 4,
                    "index_count" : 2,
                    "script_count" : 4,
                    "lang" : [
                      "painless"
                    ],
                    "lines_max" : 2,
                    "lines_total" : 6,
                    "chars_max" : 68,
                    "chars_total" : 176,
                    "source_max" : 3,
                    "source_total" : 8,
                    "doc_max" : 0,
                    "doc_total" : 0
                  },
                  {
                    "name" : "object",
                    "count" : 2,
                    "index_count" : 2,
                    "script_count" : 0
                  }
                ],
                "runtime_field_types" : [
                  {
                    "name" : "keyword",
                    "count" : 6,
                    "index_count" : 2,
                    "scriptless_count" : 2,
                    "shadowed_count" : 2,
                    "lang" : [
                      "painless"
                    ],
                    "lines_max" : 1,
                    "lines_total" : 4,
                    "chars_max" : 47,
                    "chars_total" : 118,
                    "source_max" : 1,
                    "source_total" : 2,
                    "doc_max" : 2,
                    "doc_total" : 6
                  },
                  {
                    "name" : "long",
                    "count" : 4,
                    "index_count" : 2,
                    "scriptless_count" : 0,
                    "shadowed_count" : 0,
                    "lang" : [
                      "painless"
                    ],
                    "lines_max" : 2,
                    "lines_total" : 6,
                    "chars_max" : 68,
                    "chars_total" : 176,
                    "source_max" : 3,
                    "source_total" : 8,
                    "doc_max" : 0,
                    "doc_total" : 0
                  }
                ]
              }
            }""", Strings.toString(mappingStats, true, true));
    }

    public void testToXContentWithSomeSharedMappings() {
        IndexMetadata meta = IndexMetadata.builder("index")
            .settings(SINGLE_SHARD_NO_REPLICAS)
            .putMapping(MAPPING_TEMPLATE.formatted(SCRIPT_1, SCRIPT_2, SCRIPT_3, SCRIPT_4, SCRIPT_3, SCRIPT_4, SCRIPT_1))
            .build();
        // make mappings that are slightly different because we shuffled 2 scripts between fields
        final String mappingString2 = MAPPING_TEMPLATE.formatted(SCRIPT_1, SCRIPT_2, SCRIPT_3, SCRIPT_4, SCRIPT_4, SCRIPT_3, SCRIPT_1);
        IndexMetadata meta2 = IndexMetadata.builder("index2").settings(SINGLE_SHARD_NO_REPLICAS).putMapping(mappingString2).build();
        IndexMetadata meta3 = IndexMetadata.builder("index3").settings(SINGLE_SHARD_NO_REPLICAS).putMapping(mappingString2).build();
        Metadata metadata = Metadata.builder().put(meta, false).put(meta2, false).put(meta3, false).build();
        assertThat(metadata.getMappingsByHash(), Matchers.aMapWithSize(2));
        MappingStats mappingStats = MappingStats.of(metadata, () -> {});
        assertEquals("""
            {
              "mappings" : {
                "field_types" : [
                  {
                    "name" : "keyword",
                    "count" : 6,
                    "index_count" : 3,
                    "script_count" : 3,
                    "lang" : [
                      "painless"
                    ],
                    "lines_max" : 1,
                    "lines_total" : 3,
                    "chars_max" : 47,
                    "chars_total" : 141,
                    "source_max" : 1,
                    "source_total" : 3,
                    "doc_max" : 2,
                    "doc_total" : 6
                  },
                  {
                    "name" : "long",
                    "count" : 6,
                    "index_count" : 3,
                    "script_count" : 6,
                    "lang" : [
                      "painless"
                    ],
                    "lines_max" : 2,
                    "lines_total" : 9,
                    "chars_max" : 68,
                    "chars_total" : 264,
                    "source_max" : 3,
                    "source_total" : 12,
                    "doc_max" : 0,
                    "doc_total" : 0
                  },
                  {
                    "name" : "object",
                    "count" : 3,
                    "index_count" : 3,
                    "script_count" : 0
                  }
                ],
                "runtime_field_types" : [
                  {
                    "name" : "keyword",
                    "count" : 9,
                    "index_count" : 3,
                    "scriptless_count" : 3,
                    "shadowed_count" : 3,
                    "lang" : [
                      "painless"
                    ],
                    "lines_max" : 1,
                    "lines_total" : 6,
                    "chars_max" : 47,
                    "chars_total" : 177,
                    "source_max" : 1,
                    "source_total" : 3,
                    "doc_max" : 2,
                    "doc_total" : 9
                  },
                  {
                    "name" : "long",
                    "count" : 6,
                    "index_count" : 3,
                    "scriptless_count" : 0,
                    "shadowed_count" : 0,
                    "lang" : [
                      "painless"
                    ],
                    "lines_max" : 2,
                    "lines_total" : 9,
                    "chars_max" : 68,
                    "chars_total" : 264,
                    "source_max" : 3,
                    "source_total" : 12,
                    "doc_max" : 0,
                    "doc_total" : 0
                  }
                ]
              }
            }""", Strings.toString(mappingStats, true, true));
    }

    private static String scriptAsJSON(String script) {
        return Strings.toString(new Script(script));
    }

    @Override
    protected Reader<MappingStats> instanceReader() {
        return MappingStats::new;
    }

    @Override
    protected MappingStats createTestInstance() {
        Collection<FieldStats> stats = new ArrayList<>();
        Collection<RuntimeFieldStats> runtimeFieldStats = new ArrayList<>();
        if (randomBoolean()) {
            stats.add(randomFieldStats("keyword"));
        }
        if (randomBoolean()) {
            stats.add(randomFieldStats("double"));
        }
        if (randomBoolean()) {
            runtimeFieldStats.add(randomRuntimeFieldStats("keyword"));
        }
        if (randomBoolean()) {
            runtimeFieldStats.add(randomRuntimeFieldStats("long"));
        }
        return new MappingStats(stats, runtimeFieldStats);
    }

    private static FieldStats randomFieldStats(String type) {
        FieldStats stats = new FieldStats(type);
        stats.count = randomIntBetween(0, Integer.MAX_VALUE);
        stats.indexCount = randomIntBetween(0, Integer.MAX_VALUE);
        if (randomBoolean()) {
            stats.scriptCount = randomIntBetween(0, Integer.MAX_VALUE);
            stats.scriptLangs.add(randomAlphaOfLengthBetween(3, 10));
            stats.fieldScriptStats.update(
                randomIntBetween(1, 100),
                randomLongBetween(100, 1000),
                randomIntBetween(1, 10),
                randomIntBetween(1, 10),
                randomIntBetween(1, 10)
            );
        }
        return stats;
    }

    private static RuntimeFieldStats randomRuntimeFieldStats(String type) {
        RuntimeFieldStats stats = new RuntimeFieldStats(type);
        stats.count = randomIntBetween(0, Integer.MAX_VALUE);
        stats.indexCount = randomIntBetween(0, Integer.MAX_VALUE);
        stats.scriptLessCount = randomIntBetween(0, Integer.MAX_VALUE);
        stats.shadowedCount = randomIntBetween(0, Integer.MAX_VALUE);
        stats.scriptLangs.add(randomAlphaOfLengthBetween(3, 10));
        if (randomBoolean()) {
            stats.fieldScriptStats.update(
                randomIntBetween(1, 100),
                randomLongBetween(100, 1000),
                randomIntBetween(1, 10),
                randomIntBetween(1, 10),
                randomIntBetween(1, 10)
            );
        }
        return stats;
    }

    @Override
    protected MappingStats mutateInstance(MappingStats instance) throws IOException {
        List<FieldStats> fieldTypes = new ArrayList<>(instance.getFieldTypeStats());
        List<RuntimeFieldStats> runtimeFieldTypes = new ArrayList<>(instance.getRuntimeFieldStats());
        if (randomBoolean()) {
            boolean remove = fieldTypes.size() > 0 && randomBoolean();
            if (remove) {
                fieldTypes.remove(randomInt(fieldTypes.size() - 1));
            }
            if (remove == false || randomBoolean()) {
                FieldStats s = new FieldStats("float");
                s.count = 13;
                s.indexCount = 2;
                fieldTypes.add(s);
            }
        } else {
            boolean remove = runtimeFieldTypes.size() > 0 && randomBoolean();
            if (remove) {
                runtimeFieldTypes.remove(randomInt(runtimeFieldTypes.size() - 1));
            }
            if (remove == false || randomBoolean()) {
                runtimeFieldTypes.add(randomRuntimeFieldStats("double"));
            }
        }

        return new MappingStats(fieldTypes, runtimeFieldTypes);
    }

    public void testAccountsRegularIndices() {
        String mapping = """
            {"properties":{"bar":{"type":"long"}}}""";
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 4)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        IndexMetadata.Builder indexMetadata = new IndexMetadata.Builder("foo").settings(settings).putMapping(mapping);
        Metadata metadata = new Metadata.Builder().put(indexMetadata).build();
        MappingStats mappingStats = MappingStats.of(metadata, () -> {});
        FieldStats expectedStats = new FieldStats("long");
        expectedStats.count = 1;
        expectedStats.indexCount = 1;
        assertEquals(Collections.singletonList(expectedStats), mappingStats.getFieldTypeStats());
    }

    public void testIgnoreSystemIndices() {
        String mapping = """
            {"properties":{"bar":{"type":"long"}}}""";
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 4)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        IndexMetadata.Builder indexMetadata = new IndexMetadata.Builder("foo").settings(settings).putMapping(mapping).system(true);
        Metadata metadata = new Metadata.Builder().put(indexMetadata).build();
        MappingStats mappingStats = MappingStats.of(metadata, () -> {});
        assertEquals(Collections.emptyList(), mappingStats.getFieldTypeStats());
    }

    public void testChecksForCancellation() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 4)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        IndexMetadata.Builder indexMetadata = new IndexMetadata.Builder("foo").settings(settings).putMapping("{}");
        Metadata metadata = new Metadata.Builder().put(indexMetadata).build();
        expectThrows(
            TaskCancelledException.class,
            () -> MappingStats.of(metadata, () -> { throw new TaskCancelledException("task cancelled"); })
        );
    }

    public void testWriteTo() throws IOException {
        MappingStats instance = createTestInstance();
        BytesStreamOutput out = new BytesStreamOutput();
        Version version = VersionUtils.randomCompatibleVersion(random(), Version.CURRENT);
        out.setVersion(version);
        instance.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        in.setVersion(version);
        MappingStats deserialized = new MappingStats(in);
        assertEquals(instance.getFieldTypeStats(), deserialized.getFieldTypeStats());
        assertEquals(instance.getRuntimeFieldStats(), deserialized.getRuntimeFieldStats());
    }
}
