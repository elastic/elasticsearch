/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestClusterCustomMetadata;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

        ReservedStateHandlerMetadata hmOne = new ReservedStateHandlerMetadata("one", Set.of("a", "b"));
        ReservedStateHandlerMetadata hmTwo = new ReservedStateHandlerMetadata("two", Set.of("c", "d"));

        ReservedStateErrorMetadata emOne = new ReservedStateErrorMetadata(
            1L,
            ReservedStateErrorMetadata.ErrorKind.VALIDATION,
            List.of("Test error 1", "Test error 2")
        );

        ReservedStateMetadata reservedStateMetadata = ReservedStateMetadata.builder("namespace_one")
            .errorMetadata(emOne)
            .putHandler(hmOne)
            .putHandler(hmTwo)
            .build();

        ReservedStateMetadata reservedStateMetadata1 = ReservedStateMetadata.builder("namespace_two").putHandler(hmTwo).build();

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
                ComposableIndexTemplate.builder()
                    .indexPatterns(Arrays.asList("foo", "bar*"))
                    .template(
                        new Template(
                            Settings.builder().put("setting", "value").build(),
                            new CompressedXContent("{\"baz\":\"eggplant\"}"),
                            Collections.singletonMap("alias", AliasMetadata.builder("alias").build())
                        )
                    )
                    .componentTemplates(Collections.singletonList("component_template"))
                    .priority(5L)
                    .version(4L)
                    .metadata(Collections.singletonMap("my_meta", Collections.singletonMap("potato", "chicken")))
                    .dataStreamTemplate(randomBoolean() ? null : new ComposableIndexTemplate.DataStreamTemplate())
                    .build()
            )
            .put(
                IndexMetadata.builder("test12")
                    .settings(settings(IndexVersion.current()).put("setting1", "value1").put("setting2", "value2"))
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
            .put(reservedStateMetadata)
            .put(reservedStateMetadata1)
            .build();

        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        Metadata.FORMAT.toXContent(builder, metadata);
        builder.endObject();

        Metadata parsedMetadata;
        try (var parser = createParser(builder)) {
            parsedMetadata = Metadata.Builder.fromXContent(parser);
        }

        // templates
        assertThat(parsedMetadata.getProject().templates().get("foo").name(), is("foo"));
        assertThat(parsedMetadata.getProject().templates().get("foo").patterns(), is(Collections.singletonList("bar")));
        assertThat(parsedMetadata.getProject().templates().get("foo").settings().get("index.setting1"), is("value1"));
        assertThat(parsedMetadata.getProject().templates().get("foo").settings().getByPrefix("index.").get("setting2"), is("value2"));
        assertThat(parsedMetadata.getProject().templates().get("foo").aliases().size(), equalTo(3));
        assertThat(parsedMetadata.getProject().templates().get("foo").aliases().get("alias-bar1").alias(), equalTo("alias-bar1"));
        assertThat(parsedMetadata.getProject().templates().get("foo").aliases().get("alias-bar2").alias(), equalTo("alias-bar2"));
        assertThat(
            parsedMetadata.getProject().templates().get("foo").aliases().get("alias-bar2").filter().string(),
            equalTo("{\"term\":{\"user\":\"kimchy\"}}")
        );
        assertThat(parsedMetadata.getProject().templates().get("foo").aliases().get("alias-bar3").alias(), equalTo("alias-bar3"));
        assertThat(parsedMetadata.getProject().templates().get("foo").aliases().get("alias-bar3").indexRouting(), equalTo("routing-bar"));
        assertThat(parsedMetadata.getProject().templates().get("foo").aliases().get("alias-bar3").searchRouting(), equalTo("routing-bar"));

        // component template
        assertNotNull(parsedMetadata.getProject().componentTemplates().get("component_template"));
        assertThat(parsedMetadata.getProject().componentTemplates().get("component_template").version(), is(5L));
        assertThat(
            parsedMetadata.getProject().componentTemplates().get("component_template").metadata(),
            equalTo(Collections.singletonMap("my_meta", Collections.singletonMap("foo", "bar")))
        );
        assertThat(
            parsedMetadata.getProject().componentTemplates().get("component_template").template(),
            equalTo(
                new Template(
                    Settings.builder().put("setting", "value").build(),
                    new CompressedXContent("{\"baz\":\"eggplant\"}"),
                    Collections.singletonMap("alias", AliasMetadata.builder("alias").build())
                )
            )
        );

        // index template v2
        assertNotNull(parsedMetadata.getProject().templatesV2().get("index_templatev2"));
        assertThat(parsedMetadata.getProject().templatesV2().get("index_templatev2").priority(), is(5L));
        assertThat(parsedMetadata.getProject().templatesV2().get("index_templatev2").version(), is(4L));
        assertThat(parsedMetadata.getProject().templatesV2().get("index_templatev2").indexPatterns(), is(Arrays.asList("foo", "bar*")));
        assertThat(
            parsedMetadata.getProject().templatesV2().get("index_templatev2").composedOf(),
            is(Collections.singletonList("component_template"))
        );
        assertThat(
            parsedMetadata.getProject().templatesV2().get("index_templatev2").metadata(),
            equalTo(Collections.singletonMap("my_meta", Collections.singletonMap("potato", "chicken")))
        );
        assertThat(
            parsedMetadata.getProject().templatesV2().get("index_templatev2").template(),
            equalTo(
                new Template(
                    Settings.builder().put("setting", "value").build(),
                    new CompressedXContent("{\"baz\":\"eggplant\"}"),
                    Collections.singletonMap("alias", AliasMetadata.builder("alias").build())
                )
            )
        );

        // data streams
        assertNotNull(parsedMetadata.getProject().dataStreams().get("data-stream1"));
        assertThat(parsedMetadata.getProject().dataStreams().get("data-stream1").getName(), is("data-stream1"));
        assertThat(parsedMetadata.getProject().dataStreams().get("data-stream1").getIndices(), contains(idx1.getIndex()));
        assertNotNull(parsedMetadata.getProject().dataStreams().get("data-stream2"));
        assertThat(parsedMetadata.getProject().dataStreams().get("data-stream2").getName(), is("data-stream2"));
        assertThat(parsedMetadata.getProject().dataStreams().get("data-stream2").getIndices(), contains(idx2.getIndex()));

        // reserved 'operator' metadata
        assertEquals(reservedStateMetadata, parsedMetadata.reservedStateMetadata().get(reservedStateMetadata.namespace()));
        assertEquals(reservedStateMetadata1, parsedMetadata.reservedStateMetadata().get(reservedStateMetadata1.namespace()));
    }

    private static final String MAPPING_SOURCE1 = """
        {"mapping1":{"text1":{"type":"string"}}}""";
    private static final String MAPPING_SOURCE2 = """
        {"mapping2":{"text2":{"type":"string"}}}""";
    private static final String ALIAS_FILTER1 = "{\"field1\":\"value1\"}";
    private static final String ALIAS_FILTER2 = "{\"field2\":\"value2\"}";

    public void testToXContentGateway_MultiProject() throws IOException {
        Map<String, String> mapParams = Map.of(
            Metadata.CONTEXT_MODE_PARAM,
            CONTEXT_MODE_GATEWAY,
            "flat_settings",
            "true",
            "multi-project",
            "true"
        );

        Metadata metadata = buildMetadata();
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        builder.startObject();
        ChunkedToXContent.wrapAsToXContent(metadata).toXContent(builder, new ToXContent.MapParams(mapParams));
        builder.endObject();

        assertEquals(Strings.format("""
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
                "projects" : [
                  {
                    "id" : "default",
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
                    },
                    "reserved_state" : { }
                  }
                ],
                "reserved_state" : { }
              }
            }""", IndexVersion.current(), IndexVersion.current(), IndexVersion.current()), Strings.toString(builder));
    }

    public void testToXContentGateway_FlatSettingTrue_ReduceMappingFalse() throws IOException {
        Map<String, String> mapParams = Map.of(
            Metadata.CONTEXT_MODE_PARAM,
            CONTEXT_MODE_GATEWAY,
            "flat_settings",
            "true",
            "reduce_mappings",
            "false"
        );

        Metadata metadata = buildMetadata();
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        builder.startObject();
        ChunkedToXContent.wrapAsToXContent(metadata).toXContent(builder, new ToXContent.MapParams(mapParams));
        builder.endObject();

        assertEquals(Strings.format("""
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
                },
                "reserved_state" : { }
              }
            }""", IndexVersion.current(), IndexVersion.current()), Strings.toString(builder));
    }

    public void testToXContentAPI_SameTypeName() throws IOException {
        Map<String, String> mapParams = Map.of(Metadata.CONTEXT_MODE_PARAM, CONTEXT_MODE_API);

        Metadata metadata = Metadata.builder()
            .clusterUUID("clusterUUID")
            .coordinationMetadata(CoordinationMetadata.builder().build())
            .put(
                IndexMetadata.builder("index")
                    .state(IndexMetadata.State.OPEN)
                    .settings(Settings.builder().put(SETTING_VERSION_CREATED, IndexVersion.current()))
                    .putMapping(
                        new MappingMetadata(
                            "type",
                            // the type name is the root value,
                            // the original logic in ClusterState.toXContent will reduce
                            Map.of("type", Map.of("key", "value"))
                        )
                    )
                    .numberOfShards(1)
                    .primaryTerm(0, 1L)
                    .numberOfReplicas(2)
            )
            .build();
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        builder.startObject();
        ChunkedToXContent.wrapAsToXContent(metadata).toXContent(builder, new ToXContent.MapParams(mapParams));
        builder.endObject();

        assertEquals(Strings.format("""
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
                    "mappings_updated_version" : %s,
                    "system" : false,
                    "timestamp_range" : {
                      "shards" : [ ]
                    },
                    "event_ingested_range" : {
                      "shards" : [ ]
                    }
                  }
                },
                "index-graveyard" : {
                  "tombstones" : [ ]
                },
                "reserved_state" : { }
              }
            }""", IndexVersion.current(), IndexVersion.current()), Strings.toString(builder));
    }

    public void testToXContentGateway_FlatSettingFalse_ReduceMappingTrue() throws IOException {
        Map<String, String> mapParams = Map.of(
            Metadata.CONTEXT_MODE_PARAM,
            CONTEXT_MODE_GATEWAY,
            "flat_settings",
            "false",
            "reduce_mappings",
            "true"
        );

        Metadata metadata = buildMetadata();
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        builder.startObject();
        ChunkedToXContent.wrapAsToXContent(metadata).toXContent(builder, new ToXContent.MapParams(mapParams));
        builder.endObject();

        assertEquals(Strings.format("""
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
                },
                "reserved_state" : { }
              }
            }""", IndexVersion.current(), IndexVersion.current()), Strings.toString(builder));
    }

    public void testToXContentAPI_FlatSettingTrue_ReduceMappingFalse() throws IOException {
        Map<String, String> mapParams = Map.of(
            Metadata.CONTEXT_MODE_PARAM,
            CONTEXT_MODE_API,
            "flat_settings",
            "true",
            "reduce_mappings",
            "false"
        );

        final Metadata metadata = buildMetadata();

        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        builder.startObject();
        ChunkedToXContent.wrapAsToXContent(metadata).toXContent(builder, new ToXContent.MapParams(mapParams));
        builder.endObject();

        assertEquals(Strings.format("""
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
                    "mappings_updated_version" : %s,
                    "system" : false,
                    "timestamp_range" : {
                      "shards" : [ ]
                    },
                    "event_ingested_range" : {
                      "shards" : [ ]
                    }
                  }
                },
                "index-graveyard" : {
                  "tombstones" : [ ]
                },
                "reserved_state" : { }
              }
            }""", IndexVersion.current(), IndexVersion.current(), IndexVersion.current()), Strings.toString(builder));
    }

    public void testToXContentAPI_FlatSettingFalse_ReduceMappingTrue() throws IOException {
        Map<String, String> mapParams = Map.of(
            Metadata.CONTEXT_MODE_PARAM,
            CONTEXT_MODE_API,
            "flat_settings",
            "false",
            "reduce_mappings",
            "true"
        );

        final Metadata metadata = buildMetadata();

        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        builder.startObject();
        ChunkedToXContent.wrapAsToXContent(metadata).toXContent(builder, new ToXContent.MapParams(mapParams));
        builder.endObject();

        assertEquals(Strings.format("""
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
                    "mappings_updated_version" : %s,
                    "system" : false,
                    "timestamp_range" : {
                      "shards" : [ ]
                    },
                    "event_ingested_range" : {
                      "shards" : [ ]
                    }
                  }
                },
                "index-graveyard" : {
                  "tombstones" : [ ]
                },
                "reserved_state" : { }
              }
            }""", IndexVersion.current(), IndexVersion.current(), IndexVersion.current()), Strings.toString(builder));
    }

    public void testToXContentAPIReservedMetadata() throws IOException {
        Map<String, String> mapParams = Map.of(
            Metadata.CONTEXT_MODE_PARAM,
            CONTEXT_MODE_API,
            "flat_settings",
            "false",
            "reduce_mappings",
            "true"
        );

        Metadata metadata = buildMetadata();

        ReservedStateHandlerMetadata hmOne = new ReservedStateHandlerMetadata("one", Set.of("a", "b"));
        ReservedStateHandlerMetadata hmTwo = new ReservedStateHandlerMetadata("two", Set.of("c", "d"));
        ReservedStateHandlerMetadata hmThree = new ReservedStateHandlerMetadata("three", Set.of("e", "f"));

        ReservedStateErrorMetadata emOne = new ReservedStateErrorMetadata(
            1L,
            ReservedStateErrorMetadata.ErrorKind.VALIDATION,
            List.of("Test error 1", "Test error 2")
        );

        ReservedStateErrorMetadata emTwo = new ReservedStateErrorMetadata(
            2L,
            ReservedStateErrorMetadata.ErrorKind.TRANSIENT,
            List.of("Test error 3", "Test error 4")
        );

        ReservedStateMetadata omOne = ReservedStateMetadata.builder("namespace_one")
            .errorMetadata(emOne)
            .putHandler(hmOne)
            .putHandler(hmTwo)
            .build();

        ReservedStateMetadata omTwo = ReservedStateMetadata.builder("namespace_two").errorMetadata(emTwo).putHandler(hmThree).build();

        metadata = Metadata.builder(metadata).put(omOne).put(omTwo).build();

        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        builder.startObject();
        ChunkedToXContent.wrapAsToXContent(metadata).toXContent(builder, new ToXContent.MapParams(mapParams));
        builder.endObject();

        assertEquals(Strings.format("""
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
                    "mappings_updated_version" : %s,
                    "system" : false,
                    "timestamp_range" : {
                      "shards" : [ ]
                    },
                    "event_ingested_range" : {
                      "shards" : [ ]
                    }
                  }
                },
                "index-graveyard" : {
                  "tombstones" : [ ]
                },
                "reserved_state" : {
                  "namespace_one" : {
                    "version" : -9223372036854775808,
                    "handlers" : {
                      "one" : {
                        "keys" : [
                          "a",
                          "b"
                        ]
                      },
                      "two" : {
                        "keys" : [
                          "c",
                          "d"
                        ]
                      }
                    },
                    "errors" : {
                      "version" : 1,
                      "error_kind" : "validation",
                      "errors" : [
                        "Test error 1",
                        "Test error 2"
                      ]
                    }
                  },
                  "namespace_two" : {
                    "version" : -9223372036854775808,
                    "handlers" : {
                      "three" : {
                        "keys" : [
                          "e",
                          "f"
                        ]
                      }
                    },
                    "errors" : {
                      "version" : 2,
                      "error_kind" : "transient",
                      "errors" : [
                        "Test error 3",
                        "Test error 4"
                      ]
                    }
                  }
                }
              }
            }""", IndexVersion.current(), IndexVersion.current(), IndexVersion.current()), Strings.toString(builder));
    }

    private Metadata buildMetadata() throws IOException {
        return Metadata.builder()
            .clusterUUID("clusterUUID")
            .coordinationMetadata(
                CoordinationMetadata.builder()
                    .term(1)
                    .lastCommittedConfiguration(new CoordinationMetadata.VotingConfiguration(Set.of("commitedConfigurationNodeId")))
                    .lastAcceptedConfiguration(new CoordinationMetadata.VotingConfiguration(Set.of("acceptedConfigurationNodeId")))
                    .addVotingConfigExclusion(new CoordinationMetadata.VotingConfigExclusion("exlucdedNodeId", "excludedNodeName"))
                    .build()
            )
            .persistentSettings(Settings.builder().put(SETTING_VERSION_CREATED, IndexVersion.current()).build())
            .transientSettings(Settings.builder().put(SETTING_VERSION_CREATED, IndexVersion.current()).build())
            .put(
                IndexMetadata.builder("index")
                    .state(IndexMetadata.State.OPEN)
                    .settings(Settings.builder().put(SETTING_VERSION_CREATED, IndexVersion.current()))
                    .putMapping(new MappingMetadata("type", Map.of("type1", Map.of("key", "value"))))
                    .putAlias(AliasMetadata.builder("alias").indexRouting("indexRouting").build())
                    .numberOfShards(1)
                    .primaryTerm(0, 1L)
                    .putInSyncAllocationIds(0, Set.of("allocationId"))
                    .numberOfReplicas(2)
                    .putRolloverInfo(new RolloverInfo("rolloveAlias", List.of(), 1L))
            )
            .put(
                IndexTemplateMetadata.builder("template")
                    .patterns(List.of("pattern1", "pattern2"))
                    .order(0)
                    .settings(Settings.builder().put(SETTING_VERSION_CREATED, IndexVersion.current()))
                    .putMapping("type", "{ \"key1\": {} }")
                    .build()
            )
            .build();
    }

    public static class CustomMetadata extends TestClusterCustomMetadata {
        public static final String TYPE = "custom_md";

        CustomMetadata(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY, Metadata.XContentContext.SNAPSHOT);
        }
    }
}
