/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.upgrades.FeatureMigrationResults;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;

public class ProjectMetadataTests extends ESTestCase {

    public void testToXContent() throws IOException {
        final ProjectId projectId = new ProjectId(randomUUID());
        final ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
        for (int i = 1; i <= 3; i++) {
            builder.put(
                IndexMetadata.builder(Strings.format("index-%02d", i))
                    .settings(
                        indexSettings(IndexVersion.current(), i, i % 2).put(
                            IndexMetadata.SETTING_INDEX_UUID,
                            Strings.format("i%x%04d", (i * 1000 << 16), i)
                        )
                    )
                    .putAlias(AliasMetadata.builder(Strings.format("alias.%d", i)).build())
                    .build(),
                false
            );
        }
        builder.indexTemplates(
            Map.of("template", ComposableIndexTemplate.builder().indexPatterns(List.of("index-*")).priority(10L).build())
        );

        final String dataStreamName = "logs-ultron";
        final IndexMetadata backingIndex1 = DataStreamTestHelper.createBackingIndex(dataStreamName, 1, 1725000000000L)
            .settings(
                indexSettings(IndexVersion.current(), 1, 2).put("index.hidden", true)
                    .put(IndexMetadata.SETTING_INDEX_UUID, Strings.format("d%x", 0x1000001))
            )
            .build();
        final IndexMetadata backingIndex2 = DataStreamTestHelper.createBackingIndex(dataStreamName, 2, 1725025000000L)
            .settings(
                indexSettings(IndexVersion.current(), 3, 1).put("index.hidden", true)
                    .put(IndexMetadata.SETTING_INDEX_UUID, Strings.format("d%x", 0x2000002))
            )
            .build();
        DataStream dataStream = DataStreamTestHelper.newInstance(
            dataStreamName,
            List.of(backingIndex1.getIndex(), backingIndex2.getIndex())
        );
        builder.put(backingIndex1, false);
        builder.put(backingIndex2, false);
        builder.put(dataStream);

        final ProjectMetadata projectMetadata = builder.build();

        AbstractChunkedSerializingTestCase.assertChunkCount(projectMetadata, p -> expectedChunkCount(EMPTY_PARAMS, p));

        final BytesArray expected = new BytesArray(
            Strings.format(
                """
                    {
                      "templates": {},
                      "indices": {
                        "index-01": {
                          "version": 1,
                          "mapping_version": 1,
                          "settings_version": 1,
                          "aliases_version": 1,
                          "routing_num_shards": 1,
                          "state": "open",
                          "settings": {
                            "index": {
                              "number_of_shards": "1",
                              "number_of_replicas": "1",
                              "uuid": "i3e800000001",
                              "version": {
                                "created": "%s"
                              }
                            }
                          },
                          "mappings": {},
                          "aliases": [
                            "alias.1"
                          ],
                          "primary_terms": {
                            "0": 0
                          },
                          "in_sync_allocations": {
                            "0": []
                          },
                          "rollover_info": {},
                          "mappings_updated_version": %s,
                          "system": false,
                          "timestamp_range": {
                            "shards": []
                          },
                          "event_ingested_range": {
                            "shards": []
                          }
                        },
                        "index-02": {
                          "version": 1,
                          "mapping_version": 1,
                          "settings_version": 1,
                          "aliases_version": 1,
                          "routing_num_shards": 2,
                          "state": "open",
                          "settings": {
                            "index": {
                              "number_of_shards": "2",
                              "number_of_replicas": "0",
                              "uuid": "i7d000000002",
                              "version": {
                                "created": "%s"
                              }
                            }
                          },
                          "mappings": {},
                          "aliases": [
                            "alias.2"
                          ],
                          "primary_terms": {
                            "0": 0,
                            "1": 0
                          },
                          "in_sync_allocations": {
                            "1": [],
                            "0": []
                          },
                          "rollover_info": {},
                          "mappings_updated_version": %s,
                          "system": false,
                          "timestamp_range": {
                            "shards": []
                          },
                          "event_ingested_range": {
                            "shards": []
                          }
                        },
                        "index-03": {
                          "version": 1,
                          "mapping_version": 1,
                          "settings_version": 1,
                          "aliases_version": 1,
                          "routing_num_shards": 3,
                          "state": "open",
                          "settings": {
                            "index": {
                              "number_of_shards": "3",
                              "number_of_replicas": "1",
                              "uuid": "ibb800000003",
                              "version": {
                                "created": "%s"
                              }
                            }
                          },
                          "mappings": {},
                          "aliases": [
                            "alias.3"
                          ],
                          "primary_terms": {
                            "0": 0,
                            "1": 0,
                            "2": 0
                          },
                          "in_sync_allocations": {
                            "2": [],
                            "1": [],
                            "0": []
                          },
                          "rollover_info": {},
                          "mappings_updated_version": %s,
                          "system": false,
                          "timestamp_range": {
                            "shards": []
                          },
                          "event_ingested_range": {
                            "shards": []
                          }
                        },
                        ".ds-logs-ultron-2024.08.30-000001": {
                          "version": 1,
                          "mapping_version": 1,
                          "settings_version": 1,
                          "aliases_version": 1,
                          "routing_num_shards": 1,
                          "state": "open",
                          "settings": {
                            "index": {
                              "hidden": "true",
                              "number_of_shards": "1",
                              "number_of_replicas": "2",
                              "uuid": "d1000001",
                              "version": {
                                "created": "%s"
                              }
                            }
                          },
                          "mappings": {},
                          "aliases": [],
                          "primary_terms": {
                            "0": 0
                          },
                          "in_sync_allocations": {
                            "0": []
                          },
                          "rollover_info": {},
                          "mappings_updated_version": %s,
                          "system": false,
                          "timestamp_range": {
                            "shards": []
                          },
                          "event_ingested_range": {
                            "shards": []
                          }
                        },
                        ".ds-logs-ultron-2024.08.30-000002": {
                          "version": 1,
                          "mapping_version": 1,
                          "settings_version": 1,
                          "aliases_version": 1,
                          "routing_num_shards": 3,
                          "state": "open",
                          "settings": {
                            "index": {
                              "hidden": "true",
                              "number_of_shards": "3",
                              "number_of_replicas": "1",
                              "uuid": "d2000002",
                              "version": {
                                "created": "%s"
                              }
                            }
                          },
                          "mappings": {},
                          "aliases": [],
                          "primary_terms": {
                            "0": 0,
                            "1": 0,
                            "2": 0
                          },
                          "in_sync_allocations": {
                            "0": [],
                            "1": [],
                            "2": []
                          },
                          "rollover_info": {},
                          "mappings_updated_version": %s,
                          "system": false,
                          "timestamp_range": {
                            "shards": []
                          },
                          "event_ingested_range": {
                            "shards": []
                          }
                        }
                      },
                      "index_template": {
                        "index_template": {
                          "template": {
                            "index_patterns": [
                              "index-*"
                            ],
                            "composed_of": [],
                            "priority": 10
                          }
                        }
                      },
                      "index-graveyard": {
                        "tombstones": []
                      },
                      "data_stream": {
                        "data_stream": {
                          "logs-ultron": {
                            "name": "logs-ultron",
                            "timestamp_field": {
                              "name": "@timestamp"
                            },
                            "indices": [
                              {
                                "index_name": ".ds-logs-ultron-2024.08.30-000001",
                                "index_uuid": "d1000001"
                              },
                              {
                                "index_name": ".ds-logs-ultron-2024.08.30-000002",
                                "index_uuid": "d2000002"
                              }
                            ],
                            "generation": 2,
                            "hidden": false,
                            "replicated": false,
                            "system": false,
                            "allow_custom_routing": false,
                            "failure_rollover_on_write": false,
                            "rollover_on_write": false
                          }
                        },
                        "data_stream_aliases": {}
                      }
                    }
                    """,
                IndexVersion.current(),
                IndexVersion.current(),
                IndexVersion.current(),
                IndexVersion.current(),
                IndexVersion.current(),
                IndexVersion.current(),
                IndexVersion.current(),
                IndexVersion.current(),
                IndexVersion.current(),
                IndexVersion.current()
            )
        );
        final BytesReference actual = XContentHelper.toXContent(projectMetadata, XContentType.JSON, randomBoolean());
        assertToXContentEquivalent(expected, actual, XContentType.JSON);
    }

    static int expectedChunkCount(ToXContent.Params params, ProjectMetadata project) {
        final var context = Metadata.XContentContext.from(params);

        long count = 0;
        if (context == Metadata.XContentContext.API) {
            // 2 chunks wrapping "indices"" and one chunk per index
            count += 2 + project.indices().size();
        }

        // 2 chunks wrapping "templates" and one chunk per template
        count += 2 + project.templates().size();

        for (Metadata.ProjectCustom custom : project.customs().values()) {
            count += 2;  // open / close object
            if (custom instanceof ComponentTemplateMetadata componentTemplateMetadata) {
                count += 2 + componentTemplateMetadata.componentTemplates().size();
            } else if (custom instanceof ComposableIndexTemplateMetadata composableIndexTemplateMetadata) {
                count += 2 + composableIndexTemplateMetadata.indexTemplates().size();
            } else if (custom instanceof DataStreamMetadata dataStreamMetadata) {
                count += 4 + (dataStreamMetadata.dataStreams().size() * 2L) + dataStreamMetadata.getDataStreamAliases().size();
            } else if (custom instanceof FeatureMigrationResults featureMigrationResults) {
                count += 2 + featureMigrationResults.getFeatureStatuses().size();
            } else if (custom instanceof IndexGraveyard indexGraveyard) {
                count += 2 + indexGraveyard.getTombstones().size();
            } else if (custom instanceof IngestMetadata ingestMetadata) {
                count += 2 + ingestMetadata.getPipelines().size();
            } else if (custom instanceof PersistentTasksCustomMetadata persistentTasksCustomMetadata) {
                count += 3 + persistentTasksCustomMetadata.tasks().size();
            } else {
                // could be anything, we have to just try it
                count += Iterables.size(
                    (Iterable<ToXContent>) (() -> Iterators.map(custom.toXContentChunked(params), Function.identity()))
                );
            }
        }
        return Math.toIntExact(count);
    }

}
