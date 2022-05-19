/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestCustomMetadata;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.AliasMetadata.newAliasMetadataBuilder;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createFirstBackingIndex;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.elasticsearch.cluster.metadata.Metadata.CONTEXT_MODE_API;
import static org.elasticsearch.cluster.metadata.Metadata.CONTEXT_MODE_GATEWAY;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ToAndFromJsonMetadataTests extends ESTestCase {

    public void testSimpleJsonFromAndTo() throws IOException {
        IndexMetadata idx1 = createFirstBackingIndex("data-stream1").build();
        IndexMetadata idx2 = createFirstBackingIndex("data-stream2").build();
        Metadata metadata = Metadata.builder()
            .put(
                IndexTemplateMetadata.builder("foo")
                    .patterns(Collections.singletonList("bar"))
                    .order(1)
                    .settings(Settings.builder().put("setting1", "value1").put("setting2", "value2"))
                    .putAlias(newAliasMetadataBuilder("alias-bar1"))
                    .putAlias(newAliasMetadataBuilder("alias-bar2").filter("{\"term\":{\"user\":\"kimchy\"}}"))
                    .putAlias(newAliasMetadataBuilder("alias-bar3").routing("routing-bar"))
            )
            .put(
                "component_template",
                new ComponentTemplate(
                    new Template(
                        Settings.builder().put("setting", "value").build(),
                        new CompressedXContent("{\"baz\":\"eggplant\"}"),
                        Collections.singletonMap("alias", AliasMetadata.builder("alias").build())
                    ),
                    5L,
                    Collections.singletonMap("my_meta", Collections.singletonMap("foo", "bar"))
                )
            )
            .put(
                "index_templatev2",
                new ComposableIndexTemplate(
                    Arrays.asList("foo", "bar*"),
                    new Template(
                        Settings.builder().put("setting", "value").build(),
                        new CompressedXContent("{\"baz\":\"eggplant\"}"),
                        Collections.singletonMap("alias", AliasMetadata.builder("alias").build())
                    ),
                    Collections.singletonList("component_template"),
                    5L,
                    4L,
                    Collections.singletonMap("my_meta", Collections.singletonMap("potato", "chicken")),
                    randomBoolean() ? null : new ComposableIndexTemplate.DataStreamTemplate(),
                    null
                )
            )
            .put(
                IndexMetadata.builder("test12")
                    .settings(settings(Version.CURRENT).put("setting1", "value1").put("setting2", "value2"))
                    .creationDate(2L)
                    .numberOfShards(1)
                    .numberOfReplicas(2)
                    .putMapping(MAPPING_SOURCE1)
                    .putAlias(newAliasMetadataBuilder("alias1").filter(ALIAS_FILTER1))
                    .putAlias(newAliasMetadataBuilder("alias3").writeIndex(randomBoolean() ? null : randomBoolean()))
                    .putAlias(newAliasMetadataBuilder("alias4").filter(ALIAS_FILTER2))
            )
            .put(
                IndexTemplateMetadata.builder("foo")
                    .patterns(Collections.singletonList("bar"))
                    .order(1)
                    .settings(Settings.builder().put("setting1", "value1").put("setting2", "value2"))
                    .putAlias(newAliasMetadataBuilder("alias-bar1"))
                    .putAlias(newAliasMetadataBuilder("alias-bar2").filter("{\"term\":{\"user\":\"kimchy\"}}"))
                    .putAlias(newAliasMetadataBuilder("alias-bar3").routing("routing-bar"))
            )
            .put(idx1, false)
            .put(idx2, false)
            .put(DataStreamTestHelper.newInstance("data-stream1", List.of(idx1.getIndex())))
            .put(DataStreamTestHelper.newInstance("data-stream2", List.of(idx2.getIndex())))
            .build();

        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        Metadata.FORMAT.toXContent(builder, metadata);
        builder.endObject();

        Metadata parsedMetadata = Metadata.Builder.fromXContent(createParser(builder));

        // templates
        assertThat(parsedMetadata.templates().get("foo").name(), is("foo"));
        assertThat(parsedMetadata.templates().get("foo").patterns(), is(Collections.singletonList("bar")));
        assertThat(parsedMetadata.templates().get("foo").settings().get("index.setting1"), is("value1"));
        assertThat(parsedMetadata.templates().get("foo").settings().getByPrefix("index.").get("setting2"), is("value2"));
        assertThat(parsedMetadata.templates().get("foo").aliases().size(), equalTo(3));
        assertThat(parsedMetadata.templates().get("foo").aliases().get("alias-bar1").alias(), equalTo("alias-bar1"));
        assertThat(parsedMetadata.templates().get("foo").aliases().get("alias-bar2").alias(), equalTo("alias-bar2"));
        assertThat(
            parsedMetadata.templates().get("foo").aliases().get("alias-bar2").filter().string(),
            equalTo("{\"term\":{\"user\":\"kimchy\"}}")
        );
        assertThat(parsedMetadata.templates().get("foo").aliases().get("alias-bar3").alias(), equalTo("alias-bar3"));
        assertThat(parsedMetadata.templates().get("foo").aliases().get("alias-bar3").indexRouting(), equalTo("routing-bar"));
        assertThat(parsedMetadata.templates().get("foo").aliases().get("alias-bar3").searchRouting(), equalTo("routing-bar"));

        // component template
        assertNotNull(parsedMetadata.componentTemplates().get("component_template"));
        assertThat(parsedMetadata.componentTemplates().get("component_template").version(), is(5L));
        assertThat(
            parsedMetadata.componentTemplates().get("component_template").metadata(),
            equalTo(Collections.singletonMap("my_meta", Collections.singletonMap("foo", "bar")))
        );
        assertThat(
            parsedMetadata.componentTemplates().get("component_template").template(),
            equalTo(
                new Template(
                    Settings.builder().put("setting", "value").build(),
                    new CompressedXContent("{\"baz\":\"eggplant\"}"),
                    Collections.singletonMap("alias", AliasMetadata.builder("alias").build())
                )
            )
        );

        // index template v2
        assertNotNull(parsedMetadata.templatesV2().get("index_templatev2"));
        assertThat(parsedMetadata.templatesV2().get("index_templatev2").priority(), is(5L));
        assertThat(parsedMetadata.templatesV2().get("index_templatev2").version(), is(4L));
        assertThat(parsedMetadata.templatesV2().get("index_templatev2").indexPatterns(), is(Arrays.asList("foo", "bar*")));
        assertThat(parsedMetadata.templatesV2().get("index_templatev2").composedOf(), is(Collections.singletonList("component_template")));
        assertThat(
            parsedMetadata.templatesV2().get("index_templatev2").metadata(),
            equalTo(Collections.singletonMap("my_meta", Collections.singletonMap("potato", "chicken")))
        );
        assertThat(
            parsedMetadata.templatesV2().get("index_templatev2").template(),
            equalTo(
                new Template(
                    Settings.builder().put("setting", "value").build(),
                    new CompressedXContent("{\"baz\":\"eggplant\"}"),
                    Collections.singletonMap("alias", AliasMetadata.builder("alias").build())
                )
            )
        );

        // data streams
        assertNotNull(parsedMetadata.dataStreams().get("data-stream1"));
        assertThat(parsedMetadata.dataStreams().get("data-stream1").getName(), is("data-stream1"));
        assertThat(parsedMetadata.dataStreams().get("data-stream1").getTimeStampField().getName(), is("@timestamp"));
        assertThat(parsedMetadata.dataStreams().get("data-stream1").getIndices(), contains(idx1.getIndex()));
        assertNotNull(parsedMetadata.dataStreams().get("data-stream2"));
        assertThat(parsedMetadata.dataStreams().get("data-stream2").getName(), is("data-stream2"));
        assertThat(parsedMetadata.dataStreams().get("data-stream2").getTimeStampField().getName(), is("@timestamp"));
        assertThat(parsedMetadata.dataStreams().get("data-stream2").getIndices(), contains(idx2.getIndex()));
    }

    private static final String MAPPING_SOURCE1 = """
        {"mapping1":{"text1":{"type":"string"}}}""";
    private static final String MAPPING_SOURCE2 = """
        {"mapping2":{"text2":{"type":"string"}}}""";
    private static final String ALIAS_FILTER1 = "{\"field1\":\"value1\"}";
    private static final String ALIAS_FILTER2 = "{\"field2\":\"value2\"}";

    public void testToXContentGateway_FlatSettingTrue_ReduceMappingFalse() throws IOException {
        Map<String, String> mapParams = new HashMap<>() {
            {
                put(Metadata.CONTEXT_MODE_PARAM, CONTEXT_MODE_GATEWAY);
                put("flat_settings", "true");
                put("reduce_mappings", "false");
            }
        };

        Metadata metadata = buildMetadata();
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        builder.startObject();
        metadata.toXContent(builder, new ToXContent.MapParams(mapParams));
        builder.endObject();

        assertEquals("""
            {
              "meta-data" : {
                "version" : 0,
                "cluster_uuid" : "clusterUUID",
                "cluster_uuid_committed" : false,
                "cluster_coordination" : {
                  "term" : 1,
                  "last_committed_config" : [
                    "commitedConfigurationNodeId"
                  ],
                  "last_accepted_config" : [
                    "acceptedConfigurationNodeId"
                  ],
                  "voting_config_exclusions" : [
                    {
                      "node_id" : "exlucdedNodeId",
                      "node_name" : "excludedNodeName"
                    }
                  ]
                },
                "settings" : {
                  "index.version.created" : "%s"
                },
                "templates" : {
                  "template" : {
                    "order" : 0,
                    "index_patterns" : [
                      "pattern1",
                      "pattern2"
                    ],
                    "settings" : {
                      "index.version.created" : "%s"
                    },
                    "mappings" : {
                      "key1" : { }
                    },
                    "aliases" : { }
                  }
                },
                "index-graveyard" : {
                  "tombstones" : [ ]
                }
              }
            }""".formatted(Version.CURRENT.id, Version.CURRENT.id), Strings.toString(builder));
    }

    public void testToXContentAPI_SameTypeName() throws IOException {
        Map<String, String> mapParams = new HashMap<>() {
            {
                put(Metadata.CONTEXT_MODE_PARAM, CONTEXT_MODE_API);
            }
        };

        Metadata metadata = Metadata.builder()
            .clusterUUID("clusterUUID")
            .coordinationMetadata(CoordinationMetadata.builder().build())
            .put(
                IndexMetadata.builder("index")
                    .state(IndexMetadata.State.OPEN)
                    .settings(Settings.builder().put(SETTING_VERSION_CREATED, Version.CURRENT.id))
                    .putMapping(
                        new MappingMetadata(
                            "type",
                            // the type name is the root value,
                            // the original logic in ClusterState.toXContent will reduce
                            new HashMap<>() {
                                {
                                    put("type", new HashMap<String, Object>() {
                                        {
                                            put("key", "value");
                                        }
                                    });
                                }
                            }
                        )
                    )
                    .numberOfShards(1)
                    .primaryTerm(0, 1L)
                    .numberOfReplicas(2)
            )
            .build();
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        builder.startObject();
        metadata.toXContent(builder, new ToXContent.MapParams(mapParams));
        builder.endObject();

        assertEquals("""
            {
              "metadata" : {
                "cluster_uuid" : "clusterUUID",
                "cluster_uuid_committed" : false,
                "cluster_coordination" : {
                  "term" : 0,
                  "last_committed_config" : [ ],
                  "last_accepted_config" : [ ],
                  "voting_config_exclusions" : [ ]
                },
                "templates" : { },
                "indices" : {
                  "index" : {
                    "version" : 2,
                    "mapping_version" : 1,
                    "settings_version" : 1,
                    "aliases_version" : 1,
                    "routing_num_shards" : 1,
                    "state" : "open",
                    "settings" : {
                      "index" : {
                        "number_of_shards" : "1",
                        "number_of_replicas" : "2",
                        "version" : {
                          "created" : "%s"
                        }
                      }
                    },
                    "mappings" : {
                      "type" : {
                        "key" : "value"
                      }
                    },
                    "aliases" : [ ],
                    "primary_terms" : {
                      "0" : 1
                    },
                    "in_sync_allocations" : {
                      "0" : [ ]
                    },
                    "rollover_info" : { },
                    "system" : false,
                    "timestamp_range" : {
                      "shards" : [ ]
                    }
                  }
                },
                "index-graveyard" : {
                  "tombstones" : [ ]
                }
              }
            }""".formatted(Version.CURRENT.id), Strings.toString(builder));
    }

    public void testToXContentGateway_FlatSettingFalse_ReduceMappingTrue() throws IOException {
        Map<String, String> mapParams = new HashMap<>() {
            {
                put(Metadata.CONTEXT_MODE_PARAM, CONTEXT_MODE_GATEWAY);
                put("flat_settings", "false");
                put("reduce_mappings", "true");
            }
        };

        Metadata metadata = buildMetadata();
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        builder.startObject();
        metadata.toXContent(builder, new ToXContent.MapParams(mapParams));
        builder.endObject();

        assertEquals("""
            {
              "meta-data" : {
                "version" : 0,
                "cluster_uuid" : "clusterUUID",
                "cluster_uuid_committed" : false,
                "cluster_coordination" : {
                  "term" : 1,
                  "last_committed_config" : [
                    "commitedConfigurationNodeId"
                  ],
                  "last_accepted_config" : [
                    "acceptedConfigurationNodeId"
                  ],
                  "voting_config_exclusions" : [
                    {
                      "node_id" : "exlucdedNodeId",
                      "node_name" : "excludedNodeName"
                    }
                  ]
                },
                "settings" : {
                  "index.version.created" : "%s"
                },
                "templates" : {
                  "template" : {
                    "order" : 0,
                    "index_patterns" : [
                      "pattern1",
                      "pattern2"
                    ],
                    "settings" : {
                      "index" : {
                        "version" : {
                          "created" : "%s"
                        }
                      }
                    },
                    "mappings" : { },
                    "aliases" : { }
                  }
                },
                "index-graveyard" : {
                  "tombstones" : [ ]
                }
              }
            }""".formatted(Version.CURRENT.id, Version.CURRENT.id), Strings.toString(builder));
    }

    public void testToXContentAPI_FlatSettingTrue_ReduceMappingFalse() throws IOException {
        Map<String, String> mapParams = new HashMap<>() {
            {
                put(Metadata.CONTEXT_MODE_PARAM, CONTEXT_MODE_API);
                put("flat_settings", "true");
                put("reduce_mappings", "false");
            }
        };

        final Metadata metadata = buildMetadata();

        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        builder.startObject();
        metadata.toXContent(builder, new ToXContent.MapParams(mapParams));
        builder.endObject();

        assertEquals("""
            {
              "metadata" : {
                "cluster_uuid" : "clusterUUID",
                "cluster_uuid_committed" : false,
                "cluster_coordination" : {
                  "term" : 1,
                  "last_committed_config" : [
                    "commitedConfigurationNodeId"
                  ],
                  "last_accepted_config" : [
                    "acceptedConfigurationNodeId"
                  ],
                  "voting_config_exclusions" : [
                    {
                      "node_id" : "exlucdedNodeId",
                      "node_name" : "excludedNodeName"
                    }
                  ]
                },
                "templates" : {
                  "template" : {
                    "order" : 0,
                    "index_patterns" : [
                      "pattern1",
                      "pattern2"
                    ],
                    "settings" : {
                      "index.version.created" : "%s"
                    },
                    "mappings" : {
                      "key1" : { }
                    },
                    "aliases" : { }
                  }
                },
                "indices" : {
                  "index" : {
                    "version" : 2,
                    "mapping_version" : 1,
                    "settings_version" : 1,
                    "aliases_version" : 1,
                    "routing_num_shards" : 1,
                    "state" : "open",
                    "settings" : {
                      "index.number_of_replicas" : "2",
                      "index.number_of_shards" : "1",
                      "index.version.created" : "%s"
                    },
                    "mappings" : {
                      "type" : {
                        "type1" : {
                          "key" : "value"
                        }
                      }
                    },
                    "aliases" : [
                      "alias"
                    ],
                    "primary_terms" : {
                      "0" : 1
                    },
                    "in_sync_allocations" : {
                      "0" : [
                        "allocationId"
                      ]
                    },
                    "rollover_info" : {
                      "rolloveAlias" : {
                        "met_conditions" : { },
                        "time" : 1
                      }
                    },
                    "system" : false,
                    "timestamp_range" : {
                      "shards" : [ ]
                    }
                  }
                },
                "index-graveyard" : {
                  "tombstones" : [ ]
                }
              }
            }""".formatted(Version.CURRENT.id, Version.CURRENT.id), Strings.toString(builder));
    }

    public void testToXContentAPI_FlatSettingFalse_ReduceMappingTrue() throws IOException {
        Map<String, String> mapParams = new HashMap<>() {
            {
                put(Metadata.CONTEXT_MODE_PARAM, CONTEXT_MODE_API);
                put("flat_settings", "false");
                put("reduce_mappings", "true");
            }
        };

        final Metadata metadata = buildMetadata();

        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        builder.startObject();
        metadata.toXContent(builder, new ToXContent.MapParams(mapParams));
        builder.endObject();

        assertEquals("""
            {
              "metadata" : {
                "cluster_uuid" : "clusterUUID",
                "cluster_uuid_committed" : false,
                "cluster_coordination" : {
                  "term" : 1,
                  "last_committed_config" : [
                    "commitedConfigurationNodeId"
                  ],
                  "last_accepted_config" : [
                    "acceptedConfigurationNodeId"
                  ],
                  "voting_config_exclusions" : [
                    {
                      "node_id" : "exlucdedNodeId",
                      "node_name" : "excludedNodeName"
                    }
                  ]
                },
                "templates" : {
                  "template" : {
                    "order" : 0,
                    "index_patterns" : [
                      "pattern1",
                      "pattern2"
                    ],
                    "settings" : {
                      "index" : {
                        "version" : {
                          "created" : "%s"
                        }
                      }
                    },
                    "mappings" : { },
                    "aliases" : { }
                  }
                },
                "indices" : {
                  "index" : {
                    "version" : 2,
                    "mapping_version" : 1,
                    "settings_version" : 1,
                    "aliases_version" : 1,
                    "routing_num_shards" : 1,
                    "state" : "open",
                    "settings" : {
                      "index" : {
                        "number_of_shards" : "1",
                        "number_of_replicas" : "2",
                        "version" : {
                          "created" : "%s"
                        }
                      }
                    },
                    "mappings" : {
                      "type" : {
                        "type1" : {
                          "key" : "value"
                        }
                      }
                    },
                    "aliases" : [
                      "alias"
                    ],
                    "primary_terms" : {
                      "0" : 1
                    },
                    "in_sync_allocations" : {
                      "0" : [
                        "allocationId"
                      ]
                    },
                    "rollover_info" : {
                      "rolloveAlias" : {
                        "met_conditions" : { },
                        "time" : 1
                      }
                    },
                    "system" : false,
                    "timestamp_range" : {
                      "shards" : [ ]
                    }
                  }
                },
                "index-graveyard" : {
                  "tombstones" : [ ]
                }
              }
            }""".formatted(Version.CURRENT.id, Version.CURRENT.id), Strings.toString(builder));
    }

    private Metadata buildMetadata() throws IOException {
        return Metadata.builder()
            .clusterUUID("clusterUUID")
            .coordinationMetadata(
                CoordinationMetadata.builder()
                    .term(1)
                    .lastCommittedConfiguration(new CoordinationMetadata.VotingConfiguration(new HashSet<>() {
                        {
                            add("commitedConfigurationNodeId");
                        }
                    }))
                    .lastAcceptedConfiguration(new CoordinationMetadata.VotingConfiguration(new HashSet<>() {
                        {
                            add("acceptedConfigurationNodeId");
                        }
                    }))
                    .addVotingConfigExclusion(new CoordinationMetadata.VotingConfigExclusion("exlucdedNodeId", "excludedNodeName"))
                    .build()
            )
            .persistentSettings(Settings.builder().put(SETTING_VERSION_CREATED, Version.CURRENT.id).build())
            .transientSettings(Settings.builder().put(SETTING_VERSION_CREATED, Version.CURRENT.id).build())
            .put(
                IndexMetadata.builder("index")
                    .state(IndexMetadata.State.OPEN)
                    .settings(Settings.builder().put(SETTING_VERSION_CREATED, Version.CURRENT.id))
                    .putMapping(new MappingMetadata("type", new HashMap<>() {
                        {
                            put("type1", new HashMap<String, Object>() {
                                {
                                    put("key", "value");
                                }
                            });
                        }
                    }))
                    .putAlias(AliasMetadata.builder("alias").indexRouting("indexRouting").build())
                    .numberOfShards(1)
                    .primaryTerm(0, 1L)
                    .putInSyncAllocationIds(0, new HashSet<>() {
                        {
                            add("allocationId");
                        }
                    })
                    .numberOfReplicas(2)
                    .putRolloverInfo(new RolloverInfo("rolloveAlias", new ArrayList<>(), 1L))
            )
            .put(
                IndexTemplateMetadata.builder("template")
                    .patterns(List.of("pattern1", "pattern2"))
                    .order(0)
                    .settings(Settings.builder().put(SETTING_VERSION_CREATED, Version.CURRENT.id))
                    .putMapping("type", "{ \"key1\": {} }")
                    .build()
            )
            .build();
    }

    public static class CustomMetadata extends TestCustomMetadata {
        public static final String TYPE = "custom_md";

        CustomMetadata(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY, Metadata.XContentContext.SNAPSHOT);
        }
    }
}
