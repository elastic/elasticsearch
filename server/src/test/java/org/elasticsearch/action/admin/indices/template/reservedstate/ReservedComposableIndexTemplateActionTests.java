/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.template.reservedstate;

import org.elasticsearch.action.admin.indices.template.delete.DeleteComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.delete.DeleteComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.delete.TransportDeleteComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.delete.TransportDeleteComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.metadata.ReservedStateHandlerMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettingProviders;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidIndexTemplateException;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.action.admin.indices.template.reservedstate.ReservedComposableIndexTemplateAction.componentName;
import static org.elasticsearch.action.admin.indices.template.reservedstate.ReservedComposableIndexTemplateAction.composableIndexName;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * A unit test class that tests {@link ReservedComposableIndexTemplateAction}
 */
public class ReservedComposableIndexTemplateActionTests extends ESTestCase {

    MetadataIndexTemplateService templateService;
    ClusterService clusterService;
    IndexScopedSettings indexScopedSettings;
    IndicesService indicesService;

    @Before
    public void setup() throws IOException {
        clusterService = mock(ClusterService.class);
        ClusterState state = ClusterState.builder(new ClusterName("elasticsearch")).build();
        doReturn(state).when(clusterService).state();

        final Settings settings = Settings.builder().put(IndexMetadata.SETTING_CREATION_DATE, System.currentTimeMillis()).build();
        indexScopedSettings = new IndexScopedSettings(settings, IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);

        indicesService = mock(IndicesService.class);
        var indexService = mock(IndexService.class);
        var mapperService = mock(MapperService.class);
        doReturn(mapperService).when(indexService).mapperService();
        doReturn(indexService).when(indicesService).createIndex(any(), any(), anyBoolean());

        templateService = new MetadataIndexTemplateService(
            clusterService,
            mock(MetadataCreateIndexService.class),
            indicesService,
            indexScopedSettings,
            mock(NamedXContentRegistry.class),
            mock(SystemIndices.class),
            new IndexSettingProviders(Set.of())
        );
    }

    private TransformState processJSON(
        ReservedClusterStateHandler<ReservedComposableIndexTemplateAction.ComponentsAndComposables> action,
        TransformState prevState,
        String json
    ) throws Exception {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            return action.transform(action.fromXContent(parser), prevState);
        }
    }

    public void testComponentValidation() {
        ClusterState state = ClusterState.builder(new ClusterName("elasticsearch")).build();
        TransformState prevState = new TransformState(state, Collections.emptySet());
        var action = new ReservedComposableIndexTemplateAction(templateService, indexScopedSettings);

        String badComponentJSON = """
            {
              "component_templates": {
                "template_1": {
                  "template": {
                    "_mappings": {
                      "properties": {
                        "@timestamp": {
                          "type": "date"
                        }
                      }
                    }
                  }
                }
              }
            }""";

        assertEquals(
            "[1:26] [component_template] failed to parse field [template]",
            expectThrows(XContentParseException.class, () -> processJSON(action, prevState, badComponentJSON)).getMessage()
        );
    }

    public void testComposableIndexValidation() {
        ClusterState state = ClusterState.builder(new ClusterName("elasticsearch")).build();
        TransformState prevState = new TransformState(state, Collections.emptySet());
        var action = new ReservedComposableIndexTemplateAction(templateService, indexScopedSettings);

        String badComponentJSON = """
            {
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
                  "priority": -500,
                  "composed_of": ["component_template1", "runtime_component_template"],
                  "version": 3,
                  "_meta": {
                    "description": "my custom"
                  }
                }
              }
            }""";

        assertEquals(
            "Validation Failed: 1: index template priority must be >= 0;",
            expectThrows(IllegalStateException.class, () -> processJSON(action, prevState, badComponentJSON)).getCause().getMessage()
        );

        String badComponentJSON1 = """
            {
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
                }
              }
            }""";

        assertEquals(
            "index_template [template_1] invalid, cause [index template [template_1] specifies "
                + "component templates [component_template1, runtime_component_template] that do not exist]",
            expectThrows(InvalidIndexTemplateException.class, () -> processJSON(action, prevState, badComponentJSON1)).getMessage()
        );
    }

    public void testAddRemoveComponentTemplates() throws Exception {
        ClusterState state = clusterService.state();
        TransformState prevState = new TransformState(state, Collections.emptySet());
        var action = new ReservedComposableIndexTemplateAction(templateService, indexScopedSettings);

        String emptyJSON = "";

        TransformState updatedState = processJSON(action, prevState, emptyJSON);
        assertEquals(0, updatedState.keys().size());
        assertEquals(prevState.state(), updatedState.state());

        String settingsJSON = """
            {
              "component_templates": {
                "template_1": {
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
                "template_2": {
                  "template": {
                    "mappings": {
                      "runtime": {
                        "day_of_week": {
                          "type": "keyword",
                          "script": {
                            "source": "emit(doc['@timestamp'].value.dayOfWeekEnum.getDisplayName(TextStyle.FULL, Locale.ROOT))"
                          }
                        }
                      }
                    }
                  }
                }
              }
            }""";

        prevState = updatedState;
        updatedState = processJSON(action, prevState, settingsJSON);
        assertThat(updatedState.keys(), containsInAnyOrder(componentName("template_1"), componentName("template_2")));

        String lessJSON = """
            {
              "component_templates": {
                "template_2": {
                  "template": {
                    "mappings": {
                      "runtime": {
                        "day_of_week": {
                          "type": "keyword",
                          "script": {
                            "source": "emit(doc['@timestamp'].value.dayOfWeekEnum.getDisplayName(TextStyle.FULL, Locale.ROOT))"
                          }
                        }
                      }
                    }
                  }
                }
              }
            }""";

        prevState = updatedState;
        updatedState = processJSON(action, prevState, lessJSON);
        assertThat(updatedState.keys(), containsInAnyOrder(componentName("template_2")));

        prevState = updatedState;
        updatedState = processJSON(action, prevState, emptyJSON);
        assertEquals(0, updatedState.keys().size());
    }

    public void testAddRemoveIndexTemplates() throws Exception {
        ClusterState state = clusterService.state();
        TransformState prevState = new TransformState(state, Collections.emptySet());
        var action = new ReservedComposableIndexTemplateAction(templateService, indexScopedSettings);

        String emptyJSON = "";

        TransformState updatedState = processJSON(action, prevState, emptyJSON);
        assertEquals(0, updatedState.keys().size());
        assertEquals(prevState.state(), updatedState.state());

        String settingsJSON = """
            {
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
            }""";

        prevState = updatedState;
        updatedState = processJSON(action, prevState, settingsJSON);
        assertThat(
            updatedState.keys(),
            containsInAnyOrder(
                composableIndexName("template_1"),
                composableIndexName("template_2"),
                componentName("component_template1"),
                componentName("runtime_component_template")
            )
        );

        String lessJSON = """
            {
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
                "template_2": {
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
            }""";

        prevState = updatedState;
        updatedState = processJSON(action, prevState, lessJSON);
        assertThat(
            updatedState.keys(),
            containsInAnyOrder(
                composableIndexName("template_2"),
                componentName("component_template1"),
                componentName("runtime_component_template")
            )
        );

        prevState = updatedState;
        updatedState = processJSON(action, prevState, emptyJSON);
        assertEquals(0, updatedState.keys().size());
    }

    public void testAddRemoveIndexTemplatesWithOverlap() throws Exception {
        ClusterState state = clusterService.state();
        TransformState prevState = new TransformState(state, Collections.emptySet());
        var action = new ReservedComposableIndexTemplateAction(templateService, indexScopedSettings);

        String emptyJSON = "";

        TransformState updatedState = processJSON(action, prevState, emptyJSON);
        assertEquals(0, updatedState.keys().size());
        assertEquals(prevState.state(), updatedState.state());

        // Adding two composable index templates with same index patterns will fail
        String settingsJSON = """
            {
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
                    "version": 3,
                    "_meta": {
                      "description": "my custom"
                    }
                },
                "template_2": {
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
                    "version": 3,
                    "_meta": {
                      "description": "my custom"
                    }
                }
              }
            }""";

        var prevState1 = updatedState;

        assertTrue(
            expectThrows(IllegalArgumentException.class, () -> processJSON(action, prevState1, settingsJSON)).getMessage()
                .contains(
                    "index template [template_2] has index patterns [te*, bar*] " + "matching patterns from existing templates [template_1]"
                )
        );

        var newSettingsJSON = """
            {
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
                    "version": 3,
                    "_meta": {
                      "description": "my custom"
                    }
                }
              }
            }""";

        // We add one only to see if we can replace it subsequently, inserts happen before deletes in ReservedComposableIndexTemplateAction
        prevState = updatedState;
        updatedState = processJSON(action, prevState, newSettingsJSON);
        assertThat(updatedState.keys(), containsInAnyOrder(composableIndexName("template_1")));

        String lessJSON = """
            {
              "composable_index_templates": {
                "template_2": {
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
                    "version": 3,
                    "_meta": {
                      "description": "my custom"
                    }
                }
              }
            }""";

        // We are replacing template_1 with template_2, same index pattern, no validation should be thrown
        prevState = updatedState;
        updatedState = processJSON(action, prevState, lessJSON);
        assertThat(updatedState.keys(), containsInAnyOrder(composableIndexName("template_2")));

        prevState = updatedState;
        updatedState = processJSON(action, prevState, emptyJSON);
        assertEquals(0, updatedState.keys().size());
    }

    public void testHandlerCorrectness() {
        var putIndexAction = new TransportPutComposableIndexTemplateAction(
            mock(TransportService.class),
            null,
            null,
            null,
            mock(ActionFilters.class),
            null
        );
        assertEquals(ReservedComposableIndexTemplateAction.NAME, putIndexAction.reservedStateHandlerName().get());
        assertThat(
            putIndexAction.modifiedKeys(new PutComposableIndexTemplateAction.Request("aaa")),
            containsInAnyOrder(composableIndexName("aaa"))
        );
        var delIndexAction = new TransportDeleteComposableIndexTemplateAction(
            mock(TransportService.class),
            null,
            null,
            null,
            mock(ActionFilters.class),
            null
        );
        assertEquals(ReservedComposableIndexTemplateAction.NAME, delIndexAction.reservedStateHandlerName().get());
        assertThat(
            delIndexAction.modifiedKeys(new DeleteComposableIndexTemplateAction.Request("a", "b")),
            containsInAnyOrder(composableIndexName("a"), composableIndexName("b"))
        );

        var putComponentAction = new TransportPutComponentTemplateAction(
            mock(TransportService.class),
            null,
            null,
            null,
            mock(ActionFilters.class),
            null,
            indexScopedSettings
        );
        assertEquals(ReservedComposableIndexTemplateAction.NAME, putComponentAction.reservedStateHandlerName().get());
        assertThat(
            putComponentAction.modifiedKeys(new PutComponentTemplateAction.Request("aaa")),
            containsInAnyOrder(componentName("aaa"))
        );

        var delComponentAction = new TransportDeleteComponentTemplateAction(
            mock(TransportService.class),
            null,
            null,
            null,
            mock(ActionFilters.class),
            null
        );
        assertEquals(ReservedComposableIndexTemplateAction.NAME, delComponentAction.reservedStateHandlerName().get());
        assertThat(
            delComponentAction.modifiedKeys(new DeleteComponentTemplateAction.Request("a", "b")),
            containsInAnyOrder(componentName("a"), componentName("b"))
        );
    }

    public void testBlockUsingReservedComponentTemplates() throws Exception {
        ClusterState state = clusterService.state();
        TransformState prevState = new TransformState(state, Collections.emptySet());
        var action = new ReservedComposableIndexTemplateAction(templateService, indexScopedSettings);

        String settingsJSON = """
            {
              "component_templates": {
                "template_1": {
                  "template": {
                    "mappings": {
                      "properties": {
                        "@timestamp": {
                          "type": "date"
                        }
                      }
                    }
                  }
                }
              }
            }""";

        var updatedState = processJSON(action, prevState, settingsJSON);

        Metadata metadata = Metadata.builder(updatedState.state().metadata())
            .put(
                ReservedStateMetadata.builder("test")
                    .putHandler(new ReservedStateHandlerMetadata(ReservedComposableIndexTemplateAction.NAME, updatedState.keys()))
                    .build()
            )
            .build();

        ClusterState withReservedState = new ClusterState.Builder(updatedState.state()).metadata(metadata).build();

        String composableTemplate = """
            {
              "composable_index_templates": {
                "composable_template_1": {
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
                    "composed_of": ["%s"],
                    "version": 3,
                    "_meta": {
                      "description": "my custom"
                    }
                  }
                }
              }
            }""";

        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY, String.format(Locale.ROOT, composableTemplate, "template_1"))
        ) {
            var request = action.fromXContent(parser).composableTemplates().get(0);
            assertTrue(
                expectThrows(
                    IllegalArgumentException.class,
                    () -> TransportPutComposableIndexTemplateAction.verifyIfUsingReservedComponentTemplates(request, withReservedState)
                ).getMessage().contains("errors: [[component_template:template_1] is reserved by [test]]")
            );
        }

        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY, String.format(Locale.ROOT, composableTemplate, "template_2"))
        ) {
            var request = action.fromXContent(parser).composableTemplates().get(0);
            // this should just work, no failure
            TransportPutComposableIndexTemplateAction.verifyIfUsingReservedComponentTemplates(request, withReservedState);
        }
    }
}
