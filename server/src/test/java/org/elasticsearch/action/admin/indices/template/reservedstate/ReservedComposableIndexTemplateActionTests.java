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
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettingProviders;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.IndicesService;
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
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

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

    private TransformState processJSON(ReservedClusterStateHandler<?> action, TransformState prevState, String json) throws Exception {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            return action.transform(action.fromXContent(parser), prevState);
        }
    }

    public void testComponentValidation()  {
        ClusterState state = ClusterState.builder(new ClusterName("elasticsearch")).build();
        TransformState prevState = new TransformState(state, Collections.emptySet());
        ReservedComponentTemplateAction action = new ReservedComponentTemplateAction(templateService, indexScopedSettings);

        String badComponentJSON = """
            {
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
            }""";

        assertEquals(
            "[1:26] [component_template] failed to parse field [template]",
            expectThrows(XContentParseException.class, () -> processJSON(action, prevState, badComponentJSON)).getMessage()
        );
    }

    public void testComposableIndexValidation()  {
        ClusterState state = ClusterState.builder(new ClusterName("elasticsearch")).build();
        TransformState prevState = new TransformState(state, Collections.emptySet());
        ReservedComposableIndexTemplateAction action = new ReservedComposableIndexTemplateAction(templateService);

        String badComponentJSON = """
            {
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
                      "composed_of": ["component_template1", "runtime_component_template"],\s
                      "version": 3,
                      "_meta": {
                        "description": "my custom"
                      }
                }
            }""";

        assertEquals(
            "Validation Failed: 1: index template priority must be >= 0;",
            expectThrows(IllegalStateException.class, () -> processJSON(action, prevState, badComponentJSON)).getCause().getMessage()
        );
    }

    public void testAddRemoveComponentTemplates() throws Exception {
        ClusterState state = clusterService.state();
        TransformState prevState = new TransformState(state, Collections.emptySet());
        var action = new ReservedComponentTemplateAction(templateService, indexScopedSettings);

        String emptyJSON = "";

        TransformState updatedState = processJSON(action, prevState, emptyJSON);
        assertEquals(0, updatedState.keys().size());
        assertEquals(prevState.state(), updatedState.state());

        String settingsJSON = """
            {
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
              }""";

        prevState = updatedState;
        updatedState = processJSON(action, prevState, settingsJSON);
        assertThat(updatedState.keys(), containsInAnyOrder("template_1", "template_2"));

        String lessJSON = """
            {
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
             }""";

        prevState = updatedState;
        updatedState = processJSON(action, prevState, lessJSON);
        assertThat(updatedState.keys(), containsInAnyOrder("template_2"));

        prevState = updatedState;
        updatedState = processJSON(action, prevState, emptyJSON);
        assertEquals(0, updatedState.keys().size());
    }

    public void testAddRemoveIndexTemplates() throws Exception {
        ClusterState state = clusterService.state();
        TransformState prevState = new TransformState(state, Collections.emptySet());
        var action = new ReservedComposableIndexTemplateAction(templateService);

        String emptyJSON = "";

        TransformState updatedState = processJSON(action, prevState, emptyJSON);
        assertEquals(0, updatedState.keys().size());
        assertEquals(prevState.state(), updatedState.state());

        String settingsJSON = """
            {
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
              }""";

        prevState = updatedState;
        updatedState = processJSON(action, prevState, settingsJSON);
        assertThat(updatedState.keys(), containsInAnyOrder("template_1", "template_2"));

        String lessJSON = """
            {
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
             }""";

        prevState = updatedState;
        updatedState = processJSON(action, prevState, lessJSON);
        assertThat(updatedState.keys(), containsInAnyOrder("template_2"));

        prevState = updatedState;
        updatedState = processJSON(action, prevState, emptyJSON);
        assertEquals(0, updatedState.keys().size());
    }

    public void testHandlerCorrectness() {
        var indexTemplateAction = new ReservedComposableIndexTemplateAction(templateService);
        assertThat(indexTemplateAction.dependencies(), containsInAnyOrder(ReservedComponentTemplateAction.NAME));
        var putIndexAction = new TransportPutComposableIndexTemplateAction(
            mock(TransportService.class),
            null,
            null,
            null,
            mock(ActionFilters.class),
            null
        );
        assertEquals(ReservedComposableIndexTemplateAction.NAME, putIndexAction.reservedStateHandlerName().get());
        assertThat(putIndexAction.modifiedKeys(new PutComposableIndexTemplateAction.Request("aaa")), containsInAnyOrder("aaa"));
        var delIndexAction = new TransportDeleteComposableIndexTemplateAction(
            mock(TransportService.class),
            null,
            null,
            null,
            mock(ActionFilters.class),
            null
        );
        assertEquals(ReservedComposableIndexTemplateAction.NAME, delIndexAction.reservedStateHandlerName().get());
        assertThat(delIndexAction.modifiedKeys(new DeleteComposableIndexTemplateAction.Request("a", "b")), containsInAnyOrder("a", "b"));

        var putComponentAction = new TransportPutComponentTemplateAction(mock(TransportService.class),
            null,
            null,
            null,
            mock(ActionFilters.class),
            null,
            indexScopedSettings
        );
        assertEquals(ReservedComponentTemplateAction.NAME, putComponentAction.reservedStateHandlerName().get());
        assertThat(putComponentAction.modifiedKeys(new PutComponentTemplateAction.Request("aaa")), containsInAnyOrder("aaa"));

        var delComponentAction = new TransportDeleteComponentTemplateAction(mock(TransportService.class),
            null,
            null,
            null,
            mock(ActionFilters.class),
            null
        );
        assertEquals(ReservedComponentTemplateAction.NAME, delComponentAction.reservedStateHandlerName().get());
        assertThat(delComponentAction.modifiedKeys(new DeleteComponentTemplateAction.Request("a", "b")), containsInAnyOrder("a", "b"));
    }
}
