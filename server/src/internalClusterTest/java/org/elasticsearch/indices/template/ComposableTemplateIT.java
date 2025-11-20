/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.template;

import org.elasticsearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.equalTo;

public class ComposableTemplateIT extends ESIntegTestCase {

    // See: https://github.com/elastic/elasticsearch/issues/58643
    public void testComponentTemplatesCanBeUpdatedAfterRestart() throws Exception {
        ComponentTemplate ct = new ComponentTemplate(new Template(null, new CompressedXContent("""
            {
              "dynamic": false,
              "properties": {
                "foo": {
                  "type": "text"
                }
              }
            }"""), null), 3L, Collections.singletonMap("eggplant", "potato"));
        addComponentTemplate("my-ct", ct);

        ComposableIndexTemplate cit = ComposableIndexTemplate.builder()
            .indexPatterns(Collections.singletonList("coleslaw"))
            .template(new Template(null, new CompressedXContent("""
                {
                  "dynamic": false,
                  "properties": {
                    "foo": {
                      "type": "keyword"
                    }
                  }
                }"""), null))
            .componentTemplates(Collections.singletonList("my-ct"))
            .priority(4L)
            .version(5L)
            .metadata(Collections.singletonMap("egg", "bread"))
            .build();
        addComposableTemplate("my-it", cit);

        internalCluster().fullRestart();
        ensureGreen();

        ComponentTemplate ct2 = new ComponentTemplate(new Template(null, new CompressedXContent("""
            {
              "dynamic": true,
              "properties": {
                "foo": {
                  "type": "keyword"
                }
              }
            }"""), null), 3L, Collections.singletonMap("eggplant", "potato"));
        addComponentTemplate("my-ct", ct2);

        ComposableIndexTemplate cit2 = ComposableIndexTemplate.builder()
            .indexPatterns(Collections.singletonList("coleslaw"))
            .template(new Template(null, new CompressedXContent("""
                {
                  "dynamic": true,
                  "properties": {
                    "foo": {
                      "type": "integer"
                    }
                  }
                }"""), null))
            .componentTemplates(Collections.singletonList("my-ct"))
            .priority(4L)
            .version(5L)
            .metadata(Collections.singletonMap("egg", "bread"))
            .build();
        addComposableTemplate("my-it", cit2);
    }

    public void testComposableTemplateWithSubobjectsFalseObjectAndSubfield() throws Exception {
        ComponentTemplate subobjects = new ComponentTemplate(new Template(null, new CompressedXContent("""
            {
              "properties": {
                "foo": {
                   "type": "object",
                   "subobjects": false
                 },
                 "foo.bar": {
                   "type": "keyword"
                 }
              }
            }
            """), null), null, null);

        addComponentTemplate("subobjects", subobjects);
        ComposableIndexTemplate it = ComposableIndexTemplate.builder()
            .indexPatterns(List.of("test-*"))
            .template(new Template(null, null, null))
            .componentTemplates(List.of("subobjects"))
            .priority(0L)
            .version(1L)
            .build();
        addComposableTemplate("composable-template", it);

        ProjectMetadata project = getProjectMetadata();
        List<CompressedXContent> mappings = MetadataIndexTemplateService.collectMappings(project, "composable-template", "test-index");

        assertNotNull(mappings);
        assertThat(mappings.size(), equalTo(1));
        List<Map<String, Object>> parsedMappings = mappings.stream().map(m -> {
            try {
                return MapperService.parseMapping(NamedXContentRegistry.EMPTY, m);
            } catch (Exception e) {
                logger.error(e);
                fail("failed to parse mappings: " + m.string());
                return null;
            }
        }).toList();

        assertThat(
            parsedMappings.get(0),
            equalTo(
                Map.of(
                    "_doc",
                    Map.of("properties", Map.of("foo.bar", Map.of("type", "keyword"), "foo", Map.of("type", "object", "subobjects", false)))
                )
            )
        );
    }

    public void testComposableTemplateWithSubobjectsFalse() throws Exception {
        ComponentTemplate subobjects = new ComponentTemplate(new Template(null, new CompressedXContent("""
            {
              "subobjects": false
            }
            """), null), null, null);

        ComponentTemplate fieldMapping = new ComponentTemplate(new Template(null, new CompressedXContent("""
            {
              "properties": {
                "parent.subfield": {
                  "type": "keyword"
                }
              }
            }
            """), null), null, null);

        addComponentTemplate("subobjects", subobjects);
        addComponentTemplate("field_mapping", fieldMapping);
        ComposableIndexTemplate it = ComposableIndexTemplate.builder()
            .indexPatterns(List.of("test-*"))
            .template(new Template(null, null, null))
            .componentTemplates(List.of("subobjects", "field_mapping"))
            .priority(0L)
            .version(1L)
            .build();
        addComposableTemplate("composable-template", it);

        ProjectMetadata project = getProjectMetadata();
        List<CompressedXContent> mappings = MetadataIndexTemplateService.collectMappings(project, "composable-template", "test-index");

        assertNotNull(mappings);
        assertThat(mappings.size(), equalTo(2));
        List<Map<String, Object>> parsedMappings = mappings.stream().map(m -> {
            try {
                return MapperService.parseMapping(NamedXContentRegistry.EMPTY, m);
            } catch (Exception e) {
                logger.error(e);
                fail("failed to parse mappings: " + m.string());
                return null;
            }
        }).toList();

        assertThat(parsedMappings.get(0), equalTo(Map.of("_doc", Map.of("subobjects", false))));
        assertThat(
            parsedMappings.get(1),
            equalTo(Map.of("_doc", Map.of("properties", Map.of("parent.subfield", Map.of("type", "keyword")))))
        );
    }

    public void testResolveConflictingMappings() throws Exception {
        ComponentTemplate ct1 = new ComponentTemplate(new Template(null, new CompressedXContent("""
            {
              "properties": {
                "field2": {
                  "type": "keyword"
                }
              }
            }"""), null), null, null);
        ComponentTemplate ct2 = new ComponentTemplate(new Template(null, new CompressedXContent("""
            {
              "properties": {
                "field2": {
                  "type": "text"
                }
              }
            }"""), null), null, null);
        addComponentTemplate("ct_high", ct1);
        addComponentTemplate("ct_low", ct2);
        ComposableIndexTemplate it = ComposableIndexTemplate.builder()
            .indexPatterns(List.of("i*"))
            .template(new Template(null, new CompressedXContent("""
                {
                  "properties": {
                    "field": {
                      "type": "keyword"
                    }
                  }
                }"""), null))
            .componentTemplates(List.of("ct_low", "ct_high"))
            .priority(0L)
            .version(1L)
            .build();
        addComposableTemplate("my-template", it);

        ProjectMetadata project = getProjectMetadata();
        List<CompressedXContent> mappings = MetadataIndexTemplateService.collectMappings(project, "my-template", "my-index");

        assertNotNull(mappings);
        assertThat(mappings.size(), equalTo(3));
        List<Map<String, Object>> parsedMappings = mappings.stream().map(m -> {
            try {
                return MapperService.parseMapping(NamedXContentRegistry.EMPTY, m);
            } catch (Exception e) {
                logger.error(e);
                fail("failed to parse mappings: " + m.string());
                return null;
            }
        }).toList();

        // The order of mappings should be:
        // - ct_low
        // - ct_high
        // - index template
        // Because the first elements when merging mappings have the lowest precedence
        assertThat(parsedMappings.get(0), equalTo(Map.of("_doc", Map.of("properties", Map.of("field2", Map.of("type", "text"))))));
        assertThat(parsedMappings.get(1), equalTo(Map.of("_doc", Map.of("properties", Map.of("field2", Map.of("type", "keyword"))))));
        assertThat(parsedMappings.get(2), equalTo(Map.of("_doc", Map.of("properties", Map.of("field", Map.of("type", "keyword"))))));
    }

    public void testResolveMappings() throws Exception {
        ComponentTemplate ct1 = new ComponentTemplate(new Template(null, new CompressedXContent("""
            {
              "properties": {
                "field1": {
                  "type": "keyword"
                }
              }
            }"""), null), null, null);
        ComponentTemplate ct2 = new ComponentTemplate(new Template(null, new CompressedXContent("""
            {
              "properties": {
                "field2": {
                  "type": "text"
                }
              }
            }"""), null), null, null);
        addComponentTemplate("ct_high", ct1);
        addComponentTemplate("ct_low", ct2);
        ComposableIndexTemplate it = ComposableIndexTemplate.builder()
            .indexPatterns(List.of("i*"))
            .template(new Template(null, new CompressedXContent("""
                {
                  "properties": {
                    "field3": {
                      "type": "integer"
                    }
                  }
                }"""), null))
            .componentTemplates(List.of("ct_low", "ct_high"))
            .priority(0L)
            .version(1L)
            .build();
        addComposableTemplate("my-template", it);

        ProjectMetadata project = getProjectMetadata();
        List<CompressedXContent> mappings = MetadataIndexTemplateService.collectMappings(project, "my-template", "my-index");

        assertNotNull(mappings);
        assertThat(mappings.size(), equalTo(3));
        List<Map<String, Object>> parsedMappings = mappings.stream().map(m -> {
            try {
                return MapperService.parseMapping(NamedXContentRegistry.EMPTY, m);
            } catch (Exception e) {
                logger.error(e);
                fail("failed to parse mappings: " + m.string());
                return null;
            }
        }).toList();
        assertThat(parsedMappings.get(0), equalTo(Map.of("_doc", Map.of("properties", Map.of("field2", Map.of("type", "text"))))));
        assertThat(parsedMappings.get(1), equalTo(Map.of("_doc", Map.of("properties", Map.of("field1", Map.of("type", "keyword"))))));
        assertThat(parsedMappings.get(2), equalTo(Map.of("_doc", Map.of("properties", Map.of("field3", Map.of("type", "integer"))))));
    }

    public void testDefinedTimestampMappingIsAddedForDataStreamTemplates() throws Exception {
        ComponentTemplate ct1 = new ComponentTemplate(new Template(null, new CompressedXContent("""
            {
              "properties": {
                "field1": {
                  "type": "keyword"
                }
              }
            }"""), null), null, null);

        addComponentTemplate("ct1", ct1);

        {
            ComposableIndexTemplate it = ComposableIndexTemplate.builder()
                .indexPatterns(List.of("logs*"))
                .template(new Template(null, new CompressedXContent("""
                    {
                      "properties": {
                        "field2": {
                          "type": "integer"
                        }
                      }
                    }"""), null))
                .componentTemplates(List.of("ct1"))
                .priority(0L)
                .version(1L)
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build();
            addComposableTemplate("logs-data-stream-template", it);

            ProjectMetadata project = getProjectMetadata();
            List<CompressedXContent> mappings = MetadataIndexTemplateService.collectMappings(
                project,
                "logs-data-stream-template",
                DataStream.getDefaultBackingIndexName("logs", 1L)
            );

            assertNotNull(mappings);
            assertThat(mappings.size(), equalTo(4));
            List<Map<String, Object>> parsedMappings = mappings.stream().map(m -> {
                try {
                    return MapperService.parseMapping(NamedXContentRegistry.EMPTY, m);
                } catch (Exception e) {
                    logger.error(e);
                    fail("failed to parse mappings: " + m.string());
                    return null;
                }
            }).toList();

            assertThat(
                parsedMappings.get(0),
                equalTo(
                    Map.of(
                        "_doc",
                        Map.of(
                            "properties",
                            Map.of(
                                MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD,
                                Map.of("type", "date", "ignore_malformed", "false")
                            ),
                            "_routing",
                            Map.of("required", false)
                        )
                    )
                )
            );
            assertThat(parsedMappings.get(1), equalTo(Map.of("_doc", Map.of("properties", Map.of("field1", Map.of("type", "keyword"))))));
            assertThat(parsedMappings.get(2), equalTo(Map.of("_doc", Map.of("properties", Map.of("field2", Map.of("type", "integer"))))));
        }

        {
            // indices matched by templates without the data stream field defined don't get the default @timestamp mapping
            ComposableIndexTemplate it = ComposableIndexTemplate.builder()
                .indexPatterns(List.of("timeseries*"))
                .template(new Template(null, new CompressedXContent("""
                    {
                      "properties": {
                        "field2": {
                          "type": "integer"
                        }
                      }
                    }"""), null))
                .componentTemplates(List.of("ct1"))
                .priority(0L)
                .version(1L)
                .build();
            addComposableTemplate("timeseries-template", it);

            ProjectMetadata project = getProjectMetadata();
            List<CompressedXContent> mappings = MetadataIndexTemplateService.collectMappings(project, "timeseries-template", "timeseries");

            assertNotNull(mappings);
            assertThat(mappings.size(), equalTo(2));
            List<Map<String, Object>> parsedMappings = mappings.stream().map(m -> {
                try {
                    return MapperService.parseMapping(NamedXContentRegistry.EMPTY, m);
                } catch (Exception e) {
                    logger.error(e);
                    fail("failed to parse mappings: " + m.string());
                    return null;
                }
            }).toList();

            assertThat(parsedMappings.get(0), equalTo(Map.of("_doc", Map.of("properties", Map.of("field1", Map.of("type", "keyword"))))));
            assertThat(parsedMappings.get(1), equalTo(Map.of("_doc", Map.of("properties", Map.of("field2", Map.of("type", "integer"))))));

            // a default @timestamp mapping will not be added if the matching template doesn't have the data stream field configured, even
            // if the index name matches that of a data stream backing index
            mappings = MetadataIndexTemplateService.collectMappings(
                project,
                "timeseries-template",
                DataStream.getDefaultBackingIndexName("timeseries", 1L)
            );

            assertNotNull(mappings);
            assertThat(mappings.size(), equalTo(2));
            parsedMappings = mappings.stream().map(m -> {
                try {
                    return MapperService.parseMapping(NamedXContentRegistry.EMPTY, m);
                } catch (Exception e) {
                    logger.error(e);
                    fail("failed to parse mappings: " + m.string());
                    return null;
                }
            }).toList();

            assertThat(parsedMappings.get(0), equalTo(Map.of("_doc", Map.of("properties", Map.of("field1", Map.of("type", "keyword"))))));
            assertThat(parsedMappings.get(1), equalTo(Map.of("_doc", Map.of("properties", Map.of("field2", Map.of("type", "integer"))))));
        }
    }

    public void testUserDefinedMappingTakesPrecedenceOverDefault() throws Exception {
        {
            // user defines a @timestamp mapping as part of a component template
            ComponentTemplate ct1 = new ComponentTemplate(new Template(null, new CompressedXContent("""
                {
                  "properties": {
                    "@timestamp": {
                      "type": "date_nanos"
                    }
                  }
                }"""), null), null, null);
            addComponentTemplate("ct1", ct1);

            ComposableIndexTemplate it = ComposableIndexTemplate.builder()
                .indexPatterns(List.of("logs*"))
                .componentTemplates(List.of("ct1"))
                .priority(0L)
                .version(1L)
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build();
            addComposableTemplate("logs-template", it);

            ProjectMetadata project = getProjectMetadata();
            List<CompressedXContent> mappings = MetadataIndexTemplateService.collectMappings(
                project,
                "logs-template",
                DataStream.getDefaultBackingIndexName("logs", 1L)
            );

            assertNotNull(mappings);
            assertThat(mappings.size(), equalTo(3));
            List<Map<String, Object>> parsedMappings = mappings.stream().map(m -> {
                try {
                    return MapperService.parseMapping(NamedXContentRegistry.EMPTY, m);
                } catch (Exception e) {
                    logger.error(e);
                    fail("failed to parse mappings: " + m.string());
                    return null;
                }
            }).toList();
            assertThat(
                parsedMappings.get(0),
                equalTo(
                    Map.of(
                        "_doc",
                        Map.of(
                            "properties",
                            Map.of(
                                MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD,
                                Map.of("type", "date", "ignore_malformed", "false")
                            ),
                            "_routing",
                            Map.of("required", false)
                        )
                    )
                )
            );
            assertThat(
                parsedMappings.get(1),
                equalTo(
                    Map.of(
                        "_doc",
                        Map.of("properties", Map.of(MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD, Map.of("type", "date_nanos")))
                    )
                )
            );
        }

        {
            // user defines a @timestamp mapping as part of a composable index template
            Template template = new Template(null, new CompressedXContent("""
                {
                  "properties": {
                    "@timestamp": {
                      "type": "date_nanos"
                    }
                  }
                }"""), null);
            ComposableIndexTemplate it = ComposableIndexTemplate.builder()
                .indexPatterns(List.of("timeseries*"))
                .template(template)
                .priority(0L)
                .version(1L)
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build();
            addComposableTemplate("timeseries-template", it);

            ProjectMetadata project = getProjectMetadata();
            List<CompressedXContent> mappings = MetadataIndexTemplateService.collectMappings(
                project,
                "timeseries-template",
                DataStream.getDefaultBackingIndexName("timeseries-template", 1L)
            );

            assertNotNull(mappings);
            assertThat(mappings.size(), equalTo(3));
            List<Map<String, Object>> parsedMappings = mappings.stream().map(m -> {
                try {
                    return MapperService.parseMapping(NamedXContentRegistry.EMPTY, m);
                } catch (Exception e) {
                    logger.error(e);
                    fail("failed to parse mappings: " + m.string());
                    return null;
                }
            }).toList();
            assertThat(
                parsedMappings.get(0),
                equalTo(
                    Map.of(
                        "_doc",
                        Map.of(
                            "properties",
                            Map.of(
                                MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD,
                                Map.of("type", "date", "ignore_malformed", "false")
                            ),
                            "_routing",
                            Map.of("required", false)
                        )
                    )
                )
            );
            assertThat(
                parsedMappings.get(1),
                equalTo(
                    Map.of(
                        "_doc",
                        Map.of("properties", Map.of(MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD, Map.of("type", "date_nanos")))
                    )
                )
            );
        }
    }

    public void testResolveSettings() throws Exception {
        ComponentTemplate ct1 = new ComponentTemplate(
            new Template(Settings.builder().put("number_of_replicas", 2).put("index.blocks.write", true).build(), null, null),
            null,
            null
        );
        ComponentTemplate ct2 = new ComponentTemplate(
            new Template(Settings.builder().put("index.number_of_replicas", 1).put("index.blocks.read", true).build(), null, null),
            null,
            null
        );
        addComponentTemplate("ct_high", ct1);
        addComponentTemplate("ct_low", ct2);
        ComposableIndexTemplate it = ComposableIndexTemplate.builder()
            .indexPatterns(List.of("i*"))
            .template(
                new Template(Settings.builder().put("index.blocks.write", false).put("index.number_of_shards", 3).build(), null, null)
            )
            .componentTemplates(List.of("ct_low", "ct_high"))
            .priority(0L)
            .version(1L)
            .build();
        addComposableTemplate("my-template", it);

        ProjectMetadata project = getProjectMetadata();
        Settings settings = MetadataIndexTemplateService.resolveSettings(project, "my-template");
        assertThat(settings.get("index.number_of_replicas"), equalTo("2"));
        assertThat(settings.get("index.blocks.write"), equalTo("false"));
        assertThat(settings.get("index.blocks.read"), equalTo("true"));
        assertThat(settings.get("index.number_of_shards"), equalTo("3"));
    }

    private static void addComponentTemplate(String name, ComponentTemplate template) throws InterruptedException, ExecutionException {
        client().execute(PutComponentTemplateAction.INSTANCE, new PutComponentTemplateAction.Request(name).componentTemplate(template))
            .get();
    }

    private static void addComposableTemplate(String name, ComposableIndexTemplate template) throws InterruptedException,
        ExecutionException {
        client().execute(
            TransportPutComposableIndexTemplateAction.TYPE,
            new TransportPutComposableIndexTemplateAction.Request(name).indexTemplate(template)
        ).get();
    }

    private ProjectMetadata getProjectMetadata() {
        return client().admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get().getState().metadata().getProject(ProjectId.DEFAULT);
    }
}
