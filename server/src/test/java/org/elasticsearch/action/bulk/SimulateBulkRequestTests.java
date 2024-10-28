/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static java.util.Map.entry;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class SimulateBulkRequestTests extends ESTestCase {

    public void testSerialization() throws Exception {
        testSerialization(
            getMapOrEmpty(getTestPipelineSubstitutions()),
            getMapOrEmpty(getTestComponentTemplateSubstitutions()),
            getMapOrEmpty(getTestIndexTemplateSubstitutions()),
            getMapOrEmpty(getTestMappingAddition())
        );
    }

    private <K, V> Map<K, V> getMapOrEmpty(Map<K, V> map) {
        if (randomBoolean()) {
            return map;
        } else {
            return Map.of();
        }
    }

    public void testNullsNotAllowed() {
        assertThrows(
            NullPointerException.class,
            () -> new SimulateBulkRequest(
                null,
                getTestPipelineSubstitutions(),
                getTestComponentTemplateSubstitutions(),
                getTestMappingAddition()
            )
        );
        assertThrows(
            NullPointerException.class,
            () -> new SimulateBulkRequest(
                getTestPipelineSubstitutions(),
                null,
                getTestComponentTemplateSubstitutions(),
                getTestMappingAddition()
            )
        );
        assertThrows(
            NullPointerException.class,
            () -> new SimulateBulkRequest(getTestPipelineSubstitutions(), getTestPipelineSubstitutions(), null, getTestMappingAddition())
        );
        assertThrows(
            NullPointerException.class,
            () -> new SimulateBulkRequest(
                getTestPipelineSubstitutions(),
                getTestPipelineSubstitutions(),
                getTestComponentTemplateSubstitutions(),
                null
            )
        );
    }

    private void testSerialization(
        Map<String, Map<String, Object>> pipelineSubstitutions,
        Map<String, Map<String, Object>> componentTemplateSubstitutions,
        Map<String, Map<String, Object>> indexTemplateSubstitutions,
        Map<String, Object> mappingAddition
    ) throws IOException {
        SimulateBulkRequest simulateBulkRequest = new SimulateBulkRequest(
            pipelineSubstitutions,
            componentTemplateSubstitutions,
            indexTemplateSubstitutions,
            mappingAddition
        );
        /*
         * Note: SimulateBulkRequest does not implement equals or hashCode, so we can't test serialization in the usual way for a
         * Writable
         */
        SimulateBulkRequest copy = copyWriteable(simulateBulkRequest, null, SimulateBulkRequest::new);
        assertThat(copy.getPipelineSubstitutions(), equalTo(simulateBulkRequest.getPipelineSubstitutions()));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testGetComponentTemplateSubstitutions() throws IOException {
        SimulateBulkRequest simulateBulkRequest = new SimulateBulkRequest(Map.of(), Map.of(), Map.of(), Map.of());
        assertThat(simulateBulkRequest.getComponentTemplateSubstitutions(), equalTo(Map.of()));
        String substituteComponentTemplatesString = """
              {
                  "mappings_template": {
                    "template": {
                      "mappings": {
                        "dynamic": "true",
                        "properties": {
                          "foo": {
                            "type": "keyword"
                          }
                        }
                      }
                    }
                  },
                  "settings_template": {
                    "template": {
                      "settings": {
                        "index": {
                          "default_pipeline": "bar-pipeline"
                        }
                      }
                    }
                  }
              }
            """;

        Map tempMap = XContentHelper.convertToMap(
            new BytesArray(substituteComponentTemplatesString.getBytes(StandardCharsets.UTF_8)),
            randomBoolean(),
            XContentType.JSON
        ).v2();
        Map<String, Map<String, Object>> substituteComponentTemplates = (Map<String, Map<String, Object>>) tempMap;
        simulateBulkRequest = new SimulateBulkRequest(Map.of(), substituteComponentTemplates, Map.of(), Map.of());
        Map<String, ComponentTemplate> componentTemplateSubstitutions = simulateBulkRequest.getComponentTemplateSubstitutions();
        assertThat(componentTemplateSubstitutions.size(), equalTo(2));
        assertThat(
            XContentHelper.convertToMap(
                XContentHelper.toXContent(
                    componentTemplateSubstitutions.get("mappings_template").template(),
                    XContentType.JSON,
                    randomBoolean()
                ),
                randomBoolean(),
                XContentType.JSON
            ).v2(),
            equalTo(substituteComponentTemplates.get("mappings_template").get("template"))
        );
        assertNull(componentTemplateSubstitutions.get("mappings_template").template().settings());
        assertNull(componentTemplateSubstitutions.get("settings_template").template().mappings());
        assertThat(componentTemplateSubstitutions.get("settings_template").template().settings().size(), equalTo(1));
        assertThat(
            componentTemplateSubstitutions.get("settings_template").template().settings().get("index.default_pipeline"),
            equalTo("bar-pipeline")
        );
    }

    public void testGetIndexTemplateSubstitutions() throws IOException {
        SimulateBulkRequest simulateBulkRequest = new SimulateBulkRequest(Map.of(), Map.of(), Map.of(), Map.of());
        assertThat(simulateBulkRequest.getIndexTemplateSubstitutions(), equalTo(Map.of()));
        String substituteIndexTemplatesString = """
              {
                  "foo_template": {
                    "index_patterns": ["foo*"],
                    "composed_of": ["foo_mapping_template", "foo_settings_template"],
                    "template": {
                      "mappings": {
                        "dynamic": "true",
                        "properties": {
                          "foo": {
                            "type": "keyword"
                          }
                        }
                      },
                      "settings": {
                        "index": {
                          "default_pipeline": "foo-pipeline"
                        }
                      }
                    }
                  },
                  "bar_template": {
                    "index_patterns": ["bar*"],
                    "composed_of": ["bar_mapping_template", "bar_settings_template"]
                  }
              }
            """;

        @SuppressWarnings("unchecked")
        Map<String, Map<String, Object>> substituteIndexTemplates = (Map<String, Map<String, Object>>) (Map) XContentHelper.convertToMap(
            new BytesArray(substituteIndexTemplatesString.getBytes(StandardCharsets.UTF_8)),
            randomBoolean(),
            XContentType.JSON
        ).v2();
        simulateBulkRequest = new SimulateBulkRequest(Map.of(), Map.of(), substituteIndexTemplates, Map.of());
        Map<String, ComposableIndexTemplate> indexTemplateSubstitutions = simulateBulkRequest.getIndexTemplateSubstitutions();
        assertThat(indexTemplateSubstitutions.size(), equalTo(2));
        assertThat(
            XContentHelper.convertToMap(
                XContentHelper.toXContent(indexTemplateSubstitutions.get("foo_template").template(), XContentType.JSON, randomBoolean()),
                randomBoolean(),
                XContentType.JSON
            ).v2(),
            equalTo(substituteIndexTemplates.get("foo_template").get("template"))
        );

        assertThat(indexTemplateSubstitutions.get("foo_template").template().settings().size(), equalTo(1));
        assertThat(
            indexTemplateSubstitutions.get("foo_template").template().settings().get("index.default_pipeline"),
            equalTo("foo-pipeline")
        );
        assertNull(indexTemplateSubstitutions.get("bar_template").template());
        assertNull(indexTemplateSubstitutions.get("bar_template").template());
    }

    public void testShallowClone() throws IOException {
        SimulateBulkRequest simulateBulkRequest = new SimulateBulkRequest(
            getTestPipelineSubstitutions(),
            getTestComponentTemplateSubstitutions(),
            getTestIndexTemplateSubstitutions(),
            getTestMappingAddition()
        );
        simulateBulkRequest.setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values()));
        simulateBulkRequest.waitForActiveShards(randomIntBetween(1, 10));
        simulateBulkRequest.timeout(randomTimeValue());
        simulateBulkRequest.pipeline(randomBoolean() ? null : randomAlphaOfLength(10));
        simulateBulkRequest.routing(randomBoolean() ? null : randomAlphaOfLength(10));
        simulateBulkRequest.requireAlias(randomBoolean());
        simulateBulkRequest.requireDataStream(randomBoolean());
        BulkRequest shallowCopy = simulateBulkRequest.shallowClone();
        assertThat(shallowCopy, instanceOf(SimulateBulkRequest.class));
        SimulateBulkRequest simulateBulkRequestCopy = (SimulateBulkRequest) shallowCopy;
        assertThat(simulateBulkRequestCopy.requests, equalTo(List.of()));
        assertThat(
            simulateBulkRequestCopy.getComponentTemplateSubstitutions(),
            equalTo(simulateBulkRequest.getComponentTemplateSubstitutions())
        );
        assertThat(simulateBulkRequestCopy.getPipelineSubstitutions(), equalTo(simulateBulkRequest.getPipelineSubstitutions()));
        assertThat(simulateBulkRequestCopy.getRefreshPolicy(), equalTo(simulateBulkRequest.getRefreshPolicy()));
        assertThat(simulateBulkRequestCopy.waitForActiveShards(), equalTo(simulateBulkRequest.waitForActiveShards()));
        assertThat(simulateBulkRequestCopy.timeout(), equalTo(simulateBulkRequest.timeout()));
        assertThat(shallowCopy.pipeline(), equalTo(simulateBulkRequest.pipeline()));
        assertThat(shallowCopy.routing(), equalTo(simulateBulkRequest.routing()));
        assertThat(shallowCopy.requireAlias(), equalTo(simulateBulkRequest.requireAlias()));
        assertThat(shallowCopy.requireDataStream(), equalTo(simulateBulkRequest.requireDataStream()));
    }

    private static Map<String, Map<String, Object>> getTestPipelineSubstitutions() {
        return Map.of(
            "pipeline1",
            Map.of("processors", List.of(Map.of("processor2", Map.of()), Map.of("processor3", Map.of()))),
            "pipeline2",
            Map.of("processors", List.of(Map.of("processor3", Map.of())))
        );
    }

    private static Map<String, Map<String, Object>> getTestComponentTemplateSubstitutions() {
        return Map.of(
            "template1",
            Map.of(
                "template",
                Map.of("mappings", Map.of("_source", Map.of("enabled", false), "properties", Map.of()), "settings", Map.of())
            ),
            "template2",
            Map.of("template", Map.of("mappings", Map.of(), "settings", Map.of()))
        );
    }

    private static Map<String, Map<String, Object>> getTestIndexTemplateSubstitutions() {
        return Map.of(
            "template1",
            Map.of(
                "template",
                Map.of(
                    "index_patterns",
                    List.of("foo*", "bar*"),
                    "composed_of",
                    List.of("template_1", "template_2"),
                    "mappings",
                    Map.of("_source", Map.of("enabled", false), "properties", Map.of()),
                    "settings",
                    Map.of()
                )
            ),
            "template2",
            Map.of("template", Map.of("index_patterns", List.of("foo*", "bar*"), "mappings", Map.of(), "settings", Map.of()))
        );
    }

    private static Map<String, Object> getTestMappingAddition() {
        return Map.ofEntries(
            entry(
                "_doc",
                Map.ofEntries(
                    entry("dynamic", "strict"),
                    entry(
                        "properties",
                        Map.ofEntries(
                            entry("foo", Map.ofEntries(entry("type", "keyword"))),
                            entry("bar", Map.ofEntries(entry("type", "boolean")))
                        )
                    )
                )
            )
        );
    }
}
