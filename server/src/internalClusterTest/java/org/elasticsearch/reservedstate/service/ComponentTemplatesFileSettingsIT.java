/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reservedstate.service;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.template.get.GetComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.get.GetComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.reservedstate.ReservedComposableIndexTemplateAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.ReservedStateErrorMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateHandlerMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.action.admin.indices.template.reservedstate.ReservedComposableIndexTemplateAction.reservedComposableIndexName;
import static org.elasticsearch.test.NodeRoles.dataOnlyNode;
import static org.elasticsearch.xcontent.XContentType.JSON;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
@LuceneTestCase.SuppressFileSystems("*")
public class ComponentTemplatesFileSettingsIT extends ESIntegTestCase {

    private static final AtomicLong versionCounter = new AtomicLong(1);

    private static final String emptyJSON = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {
                "index_templates": {
                }
             }
        }""";

    private static final String testJSON = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {
               "index_templates": {
                 "component_templates": {
                    "component_template1": {
                      "template": {
                        "mappings": {
                          "properties": {
                            "@timestamp": {
                              "type": "date"
                            }
                          }
                        }
                      }
                    },
                    "runtime_component_template": {
                      "template": {
                        "mappings": {
                          "runtime": {
                            "day_of_week": {
                              "type": "keyword"
                            }
                          }
                        }
                      }
                    },
                    "other_component_template": {
                      "template": {
                        "mappings": {
                          "runtime": {
                            "day_of_week": {
                              "type": "keyword"
                            }
                          }
                        }
                      }
                    }
                 },
                 "composable_index_templates": {
                    "template_1": {
                        "index_patterns": ["te*", "bar*"],
                        "template": {
                          "settings": {
                            "number_of_shards": 1
                          },
                          "mappings": {
                            "_source": {
                              "enabled": true
                            },
                            "properties": {
                              "host_name": {
                                "type": "keyword"
                              },
                              "created_at": {
                                "type": "date",
                                "format": "EEE MMM dd HH:mm:ss Z yyyy"
                              }
                            }
                          },
                          "aliases": {
                            "mydata": { }
                          }
                        },
                        "priority": 500,
                        "composed_of": ["component_template1", "runtime_component_template"],
                        "version": 3,
                        "_meta": {
                          "description": "my custom"
                        }
                    },
                    "template_2": {
                        "index_patterns": ["foo*", "mar*"],
                        "template": {
                          "settings": {
                            "number_of_shards": 1
                          },
                          "mappings": {
                            "_source": {
                              "enabled": true
                            },
                            "properties": {
                              "host_name": {
                                "type": "keyword"
                              },
                              "created_at": {
                                "type": "date",
                                "format": "EEE MMM dd HH:mm:ss Z yyyy"
                              }
                            }
                          },
                          "aliases": {
                            "mydata": { }
                          }
                        },
                        "priority": 100,
                        "composed_of": ["component_template1", "runtime_component_template"],
                        "version": 3,
                        "_meta": {
                          "description": "my custom"
                        }
                    },
                    "template_other": {
                        "index_patterns": ["foo*", "mar*"],
                        "template": {
                          "settings": {
                            "number_of_shards": 1
                          },
                          "mappings": {
                            "_source": {
                              "enabled": true
                            },
                            "properties": {
                              "host_name": {
                                "type": "keyword"
                              },
                              "created_at": {
                                "type": "date",
                                "format": "EEE MMM dd HH:mm:ss Z yyyy"
                              }
                            }
                          },
                          "aliases": {
                            "mydata": { }
                          }
                        },
                        "priority": 50,
                        "composed_of": ["other_component_template"],
                        "version": 3,
                        "_meta": {
                          "description": "my custom"
                        }
                    }
                 }
               }
             }
        }""";

    private static final String testJSONLess = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {
               "index_templates": {
                 "component_templates": {
                    "component_template1": {
                      "template": {
                        "mappings": {
                          "properties": {
                            "@timestamp": {
                              "type": "date"
                            }
                          }
                        }
                      }
                    },
                    "runtime_component_template": {
                      "template": {
                        "mappings": {
                          "runtime": {
                            "day_of_week": {
                              "type": "keyword"
                            }
                          }
                        }
                      }
                    }
                 },
                 "composable_index_templates": {
                    "template_1": {
                        "index_patterns": ["te*", "bar*"],
                        "template": {
                          "settings": {
                            "number_of_shards": 1
                          },
                          "mappings": {
                            "_source": {
                              "enabled": true
                            },
                            "properties": {
                              "host_name": {
                                "type": "keyword"
                              },
                              "created_at": {
                                "type": "date",
                                "format": "EEE MMM dd HH:mm:ss Z yyyy"
                              }
                            }
                          },
                          "aliases": {
                            "mydata": { }
                          }
                        },
                        "priority": 500,
                        "composed_of": ["component_template1", "runtime_component_template"],
                        "version": 3,
                        "_meta": {
                          "description": "my custom"
                        }
                    },
                    "template_2": {
                        "index_patterns": ["foo*", "mar*"],
                        "template": {
                          "settings": {
                            "number_of_shards": 1
                          },
                          "mappings": {
                            "_source": {
                              "enabled": true
                            },
                            "properties": {
                              "host_name": {
                                "type": "keyword"
                              },
                              "created_at": {
                                "type": "date",
                                "format": "EEE MMM dd HH:mm:ss Z yyyy"
                              }
                            }
                          },
                          "aliases": {
                            "mydata": { }
                          }
                        },
                        "priority": 100,
                        "composed_of": ["component_template1", "runtime_component_template"],
                        "version": 3,
                        "_meta": {
                          "description": "my custom"
                        }
                    }
                 }
               }
             }
        }""";

    private static final String testErrorJSON = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {
               "index_templates": {
                 "composable_index_templates": {
                    "err_template": {
                        "index_patterns": ["te*", "bar*"],
                        "template": {
                          "settings": {
                            "number_of_shards": 1
                          },
                          "mappings": {
                            "_source": {
                              "enabled": true
                            },
                            "properties": {
                              "host_name": {
                                "type": "keyword"
                              },
                              "created_at": {
                                "type": "date",
                                "format": "EEE MMM dd HH:mm:ss Z yyyy"
                              }
                            }
                          },
                          "aliases": {
                            "mydata": { }
                          }
                        },
                        "priority": 500,
                        "composed_of": ["component_template1", "runtime_component_template"],
                        "version": 3,
                        "_meta": {
                          "description": "my custom"
                        }
                    }
                 }
               }
             }
        }""";

    private void assertMasterNode(Client client, String node) throws ExecutionException, InterruptedException {
        assertThat(
            client.admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).execute().get().getState().nodes().getMasterNode().getName(),
            equalTo(node)
        );
    }

    private void writeJSONFile(String node, String json) throws Exception {
        FileSettingsServiceIT.writeJSONFile(node, json, logger, versionCounter.incrementAndGet());
    }

    private Tuple<CountDownLatch, AtomicLong> setupClusterStateListener(String node) {
        ClusterService clusterService = internalCluster().clusterService(node);
        CountDownLatch savedClusterState = new CountDownLatch(1);
        AtomicLong metadataVersion = new AtomicLong(-1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                ReservedStateMetadata reservedState = event.state().metadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE);
                if (reservedState != null) {
                    ReservedStateHandlerMetadata handlerMetadata = reservedState.handlers().get(ReservedComposableIndexTemplateAction.NAME);
                    if (handlerMetadata != null && handlerMetadata.keys().contains(reservedComposableIndexName("template_1"))) {
                        clusterService.removeListener(this);
                        metadataVersion.set(event.state().metadata().version());
                        savedClusterState.countDown();
                    } else if (reservedState.errorMetadata() != null) {
                        clusterService.removeListener(this);
                        savedClusterState.countDown();
                        throw new IllegalStateException(String.join(",", reservedState.errorMetadata().errors()));
                    }
                }
            }
        });

        return new Tuple<>(savedClusterState, metadataVersion);
    }

    private void assertClusterStateSaveOK(CountDownLatch savedClusterState, AtomicLong metadataVersion) throws Exception {
        boolean awaitSuccessful = savedClusterState.await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);

        final ClusterStateResponse clusterStateResponse = clusterAdmin().state(
            new ClusterStateRequest(TEST_REQUEST_TIMEOUT).waitForMetadataVersion(metadataVersion.get())
        ).actionGet();

        Map<String, ComposableIndexTemplate> allTemplates = clusterStateResponse.getState().metadata().getProject().templatesV2();

        assertThat(allTemplates.keySet(), containsInAnyOrder("template_1", "template_2", "template_other"));

        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                client().execute(PutComponentTemplateAction.INSTANCE, sampleComponentRestRequest("component_template1"))
            ).getMessage(),
            containsString("[[component_template:component_template1] set as read-only by [file_settings]]")
        );

        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                client().execute(TransportPutComposableIndexTemplateAction.TYPE, sampleIndexTemplateRestRequest("template_1"))
            ).getMessage(),
            containsString("[[composable_index_template:template_1] set as read-only by [file_settings]]")
        );
    }

    private Tuple<CountDownLatch, AtomicLong> setupClusterStateListenerForOtherDelete(String node) {
        ClusterService clusterService = internalCluster().clusterService(node);
        CountDownLatch savedClusterState = new CountDownLatch(1);
        AtomicLong metadataVersion = new AtomicLong(-1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                ReservedStateMetadata reservedState = event.state().metadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE);
                if (reservedState != null) {
                    ReservedStateHandlerMetadata handlerMetadata = reservedState.handlers().get(ReservedComposableIndexTemplateAction.NAME);
                    if (handlerMetadata != null
                        && handlerMetadata.keys().isEmpty() == false
                        && handlerMetadata.keys().contains(reservedComposableIndexName("template_other")) == false) {
                        clusterService.removeListener(this);
                        metadataVersion.set(event.state().metadata().version());
                        savedClusterState.countDown();
                    } else if (reservedState.errorMetadata() != null) {
                        clusterService.removeListener(this);
                        savedClusterState.countDown();
                        throw new IllegalStateException(String.join(",", reservedState.errorMetadata().errors()));
                    }
                }
            }
        });

        return new Tuple<>(savedClusterState, metadataVersion);
    }

    private void assertComponentAndIndexTemplateDelete(CountDownLatch savedClusterState, AtomicLong metadataVersion) throws Exception {
        boolean awaitSuccessful = savedClusterState.await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);

        final var response = client().execute(
            GetComposableIndexTemplateAction.INSTANCE,
            new GetComposableIndexTemplateAction.Request(TEST_REQUEST_TIMEOUT, "template*")
        ).get();

        assertThat(response.indexTemplates().keySet(), containsInAnyOrder("template_1", "template_2"));

        final var componentResponse = client().execute(
            GetComponentTemplateAction.INSTANCE,
            new GetComponentTemplateAction.Request(TEST_REQUEST_TIMEOUT, "other*")
        ).get();

        assertThat(componentResponse.getComponentTemplates(), anEmptyMap());

        // this should just work, other is not locked
        client().execute(PutComponentTemplateAction.INSTANCE, sampleComponentRestRequest("other_component_template")).get();

        // this will fail now because sampleIndexTemplateRestRequest wants use the component templates
        // ["component_template1", "runtime_component_template"], which are not allowed to be used by REST requests, since they
        // are written by file based settings, e.g. in operator mode. Allowing REST requests to use these components would mean that
        // we would be unable to delete these components with file based settings, since they would be used by various composable
        // index templates not managed by file based settings.
        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                client().execute(TransportPutComposableIndexTemplateAction.TYPE, sampleIndexTemplateRestRequest("template_other"))
            ).getMessage(),
            containsString(
                "with errors: [[component_template:runtime_component_template, "
                    + "component_template:component_template1] is reserved by [file_settings]]"
            )
        );

        // this will work now, we are saving template without components
        client().execute(TransportPutComposableIndexTemplateAction.TYPE, sampleIndexTemplateRestRequestNoComponents("template_other"))
            .get();

        // the rest are still locked
        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                client().execute(PutComponentTemplateAction.INSTANCE, sampleComponentRestRequest("component_template1"))
            ).getMessage(),
            containsString("[[component_template:component_template1] set as read-only by [file_settings]]")
        );

        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                client().execute(TransportPutComposableIndexTemplateAction.TYPE, sampleIndexTemplateRestRequest("template_1"))
            ).getMessage(),
            containsString("[[composable_index_template:template_1] set as read-only by [file_settings]]")
        );
    }

    private Tuple<CountDownLatch, AtomicLong> setupClusterStateListenerForCleanup(String node) {
        ClusterService clusterService = internalCluster().clusterService(node);
        CountDownLatch savedClusterState = new CountDownLatch(1);
        AtomicLong metadataVersion = new AtomicLong(-1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                ReservedStateMetadata reservedState = event.state().metadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE);
                if (reservedState != null) {
                    ReservedStateHandlerMetadata handlerMetadata = reservedState.handlers().get(ReservedComposableIndexTemplateAction.NAME);
                    if (handlerMetadata == null || handlerMetadata.keys().isEmpty()) {
                        clusterService.removeListener(this);
                        metadataVersion.set(event.state().metadata().version());
                        savedClusterState.countDown();
                    }
                }
            }
        });

        return new Tuple<>(savedClusterState, metadataVersion);
    }

    public void testSettingsApplied() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start data node / non master node");
        var dataNode = internalCluster().startNode(Settings.builder().put(dataOnlyNode()).put("discovery.initial_state_timeout", "1s"));

        var savedClusterState = setupClusterStateListener(dataNode);
        // In internal cluster tests, the nodes share the config directory, so when we write with the data node path
        // the master will pick it up on start
        logger.info("--> write the initial settings json with all component templates and composable index templates");
        writeJSONFile(dataNode, testJSON);

        logger.info("--> start master node");
        final String masterNode = internalCluster().startMasterOnlyNode();
        assertMasterNode(internalCluster().nonMasterClient(), masterNode);

        assertClusterStateSaveOK(savedClusterState.v1(), savedClusterState.v2());

        savedClusterState = setupClusterStateListenerForOtherDelete(internalCluster().getMasterName());
        logger.info("--> write the reduced JSON, so we delete template_other and other_component_template");
        writeJSONFile(internalCluster().getMasterName(), testJSONLess);

        assertComponentAndIndexTemplateDelete(savedClusterState.v1(), savedClusterState.v2());

        logger.info("---> cleanup file based settings...");
        // if clean-up doesn't succeed correctly, TestCluster.wipeAllComposableIndexTemplates will fail
        savedClusterState = setupClusterStateListenerForCleanup(internalCluster().getMasterName());
        writeJSONFile(internalCluster().getMasterName(), emptyJSON);
        boolean awaitSuccessful = savedClusterState.v1().await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);
    }

    private Tuple<CountDownLatch, AtomicLong> setupClusterStateListenerForError(String node) {
        ClusterService clusterService = internalCluster().clusterService(node);
        CountDownLatch savedClusterState = new CountDownLatch(1);
        AtomicLong metadataVersion = new AtomicLong(-1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                ReservedStateMetadata reservedState = event.state().metadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE);
                if (reservedState != null && reservedState.errorMetadata() != null) {
                    assertEquals(ReservedStateErrorMetadata.ErrorKind.VALIDATION, reservedState.errorMetadata().errorKind());
                    assertThat(reservedState.errorMetadata().errors(), allOf(notNullValue(), hasSize(1)));
                    assertThat(
                        reservedState.errorMetadata().errors().get(0),
                        containsString(
                            "index_template [err_template] invalid, cause [index template [err_template] specifies "
                                + "component templates [component_template1, runtime_component_template] that do not exist]"
                        )
                    );
                    clusterService.removeListener(this);
                    metadataVersion.set(event.state().metadata().version());
                    savedClusterState.countDown();
                }
            }
        });

        return new Tuple<>(savedClusterState, metadataVersion);
    }

    private void assertClusterStateNotSaved(CountDownLatch savedClusterState, AtomicLong metadataVersion) throws Exception {
        boolean awaitSuccessful = savedClusterState.await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);

        final var response = client().execute(
            GetComposableIndexTemplateAction.INSTANCE,
            new GetComposableIndexTemplateAction.Request(TEST_REQUEST_TIMEOUT, "err*")
        ).get();

        assertThat(response.indexTemplates(), anEmptyMap());

        // This should succeed, nothing was reserved
        client().execute(TransportPutComposableIndexTemplateAction.TYPE, sampleIndexTemplateRestRequestNoComponents("err_template")).get();
    }

    public void testErrorSaved() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start data node / non master node");
        internalCluster().startNode(Settings.builder().put(dataOnlyNode()).put("discovery.initial_state_timeout", "1s"));

        logger.info("--> start master node");
        final String masterNode = internalCluster().startMasterOnlyNode();
        assertMasterNode(internalCluster().nonMasterClient(), masterNode);
        var savedClusterState = setupClusterStateListenerForError(masterNode);

        writeJSONFile(masterNode, testErrorJSON);
        assertClusterStateNotSaved(savedClusterState.v1(), savedClusterState.v2());
    }

    private PutComponentTemplateAction.Request sampleComponentRestRequest(String name) throws Exception {
        var json = """
            {
              "template": {
                "mappings": {
                  "properties": {
                    "@timestamp": {
                      "type": "date"
                    }
                  }
                }
              }
            }""";

        try (
            var bis = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
            var parser = JSON.xContent().createParser(XContentParserConfiguration.EMPTY, bis)
        ) {
            return new PutComponentTemplateAction.Request(name).componentTemplate(ComponentTemplate.parse(parser));
        }
    }

    private TransportPutComposableIndexTemplateAction.Request sampleIndexTemplateRestRequest(String name) throws Exception {
        var json = """
            {
                "index_patterns": ["te*", "bar*"],
                "template": {
                  "settings": {
                    "number_of_shards": 1
                  },
                  "mappings": {
                    "_source": {
                      "enabled": true
                    },
                    "properties": {
                      "host_name": {
                        "type": "keyword"
                      },
                      "created_at": {
                        "type": "date",
                        "format": "EEE MMM dd HH:mm:ss Z yyyy"
                      }
                    }
                  },
                  "aliases": {
                    "mydata": { }
                  }
                },
                "priority": 500,
                "composed_of": ["component_template1", "runtime_component_template"],
                "version": 3,
                "_meta": {
                  "description": "my custom"
                }
            }""";

        try (
            var bis = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
            var parser = JSON.xContent().createParser(XContentParserConfiguration.EMPTY, bis)
        ) {
            return new TransportPutComposableIndexTemplateAction.Request(name).indexTemplate(ComposableIndexTemplate.parse(parser));
        }
    }

    private TransportPutComposableIndexTemplateAction.Request sampleIndexTemplateRestRequestNoComponents(String name) throws Exception {
        var json = """
            {
                "index_patterns": ["aa*", "vv*"],
                "template": {
                  "settings": {
                    "number_of_shards": 1
                  },
                  "mappings": {
                    "_source": {
                      "enabled": true
                    },
                    "properties": {
                      "host_name": {
                        "type": "keyword"
                      },
                      "created_at": {
                        "type": "date",
                        "format": "EEE MMM dd HH:mm:ss Z yyyy"
                      }
                    }
                  },
                  "aliases": {
                    "mydata": { }
                  }
                },
                "priority": 500,
                "version": 3,
                "_meta": {
                  "description": "my custom"
                }
            }""";

        try (
            var bis = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
            var parser = JSON.xContent().createParser(XContentParserConfiguration.EMPTY, bis)
        ) {
            return new TransportPutComposableIndexTemplateAction.Request(name).indexTemplate(ComposableIndexTemplate.parse(parser));
        }
    }

}
