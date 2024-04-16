/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequestBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class DynamicFieldMapperTests extends ESSingleNodeTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(NonDynamicFieldPlugin.class);
    }

    public void testCreateExplicitMappingSucceeds() throws Exception {
        String mapping = """
            {
              "_doc": {
                "properties": {
                  "field": {
                    "type": "non_dynamic"
                  }
                }
              }
            }
            """;
        var resp = client().admin().indices().prepareCreate("test").setMapping(mapping).get();
        assertTrue(resp.isAcknowledged());
        var mappingsResp = client().admin().indices().prepareGetMappings("test").get();
        var mappingMetadata = mappingsResp.getMappings().get("test");
        var fieldType = XContentMapValues.extractValue("properties.field.type", mappingMetadata.getSourceAsMap());
        assertThat(fieldType, equalTo(NonDynamicFieldMapper.NAME));
    }

    public void testCreateDynamicMappingFails() throws Exception {
        String mapping = """
            {
              "_doc": {
                  "dynamic_templates": [
                    {
                      "strings_as_type": {
                        "match_mapping_type": "string",
                        "mapping": {
                          "type": "non_dynamic"
                        }
                      }
                    }
                  ]
                }
            }
            """;
        CreateIndexRequestBuilder req = client().admin().indices().prepareCreate("test").setMapping(mapping);
        Exception exc = expectThrows(Exception.class, () -> req.get());
        assertThat(exc.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(exc.getCause().getCause(), instanceOf(MapperParsingException.class));
        assertThat(exc.getCause().getCause().getMessage(), containsString("[non_dynamic] can't be used in dynamic templates"));
    }

    public void testUpdateDynamicMappingFails() throws Exception {
        var resp = client().admin().indices().prepareCreate("test").get();
        assertTrue(resp.isAcknowledged());
        String mapping = """
            {
              "_doc": {
                  "dynamic_templates": [
                    {
                      "strings_as_type": {
                        "match_mapping_type": "string",
                        "mapping": {
                          "type": "non_dynamic"
                        }
                      }
                    }
                  ]
                }
            }
            """;
        var req = client().admin().indices().preparePutMapping("test").setSource(mapping, XContentType.JSON);
        Exception exc = expectThrows(Exception.class, () -> req.get());
        assertThat(exc.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(exc.getCause().getCause(), instanceOf(MapperParsingException.class));
        assertThat(exc.getCause().getCause().getMessage(), containsString("[non_dynamic] can't be used in dynamic templates"));
    }

    public void testCreateDynamicMappingInIndexTemplateFails() throws Exception {
        String mapping = """
            {
              "_doc": {
                "dynamic_templates": [
                  {
                    "strings_as_type": {
                      "match_mapping_type": "string",
                      "mapping": {
                        "type": "non_dynamic"
                      }
                    }
                  }
                ]
              }
            }
            """;
        PutIndexTemplateRequestBuilder req = client().admin()
            .indices()
            .preparePutTemplate("template1")
            .setMapping(mapping, XContentType.JSON)
            .setPatterns(List.of("test*"));
        Exception exc = expectThrows(Exception.class, () -> req.get());
        assertThat(exc.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(exc.getCause().getCause(), instanceOf(MapperParsingException.class));
        assertThat(exc.getCause().getCause().getMessage(), containsString("[non_dynamic] can't be used in dynamic templates"));
    }

    public void testCreateExplicitMappingInIndexTemplateSucceeds() throws Exception {
        String mapping = """
            {
              "_doc": {
                "properties": {
                  "field": {
                    "type": "non_dynamic"
                  }
                }
              }
            }
            """;
        PutIndexTemplateRequestBuilder req = client().admin()
            .indices()
            .preparePutTemplate("template1")
            .setMapping(mapping, XContentType.JSON)
            .setPatterns(List.of("test*"));
        assertTrue(req.get().isAcknowledged());

        var resp = client().prepareIndex("test1").setSource("field", "hello world").get();
        assertThat(resp.status(), equalTo(RestStatus.CREATED));

        var mappingsResp = client().admin().indices().prepareGetMappings("test1").get();
        var mappingMetadata = mappingsResp.getMappings().get("test1");
        var fieldType = XContentMapValues.extractValue("properties.field.type", mappingMetadata.getSourceAsMap());
        assertThat(fieldType, equalTo(NonDynamicFieldMapper.NAME));
    }

    public static class NonDynamicFieldPlugin extends Plugin implements MapperPlugin {
        public NonDynamicFieldPlugin() {}

        @Override
        public Map<String, Mapper.TypeParser> getMappers() {
            return Map.of(NonDynamicFieldMapper.NAME, NonDynamicFieldMapper.PARSER);
        }
    }

    private static class NonDynamicFieldMapper extends FieldMapper {
        private static final String NAME = "non_dynamic";

        private static final TypeParser PARSER = new TypeParser(
            (n, c) -> new Builder(n),
            List.of(notFromDynamicTemplates(NAME), notInMultiFields(NAME))
        );

        private static class Builder extends FieldMapper.Builder {
            private final Parameter<Map<String, String>> meta = Parameter.metaParam();

            Builder(String name) {
                super(name);
            }

            @Override
            protected Parameter<?>[] getParameters() {
                return new Parameter<?>[] { meta };
            }

            @Override
            public NonDynamicFieldMapper build(MapperBuilderContext context) {
                return new NonDynamicFieldMapper(name(), new TextFieldMapper.TextFieldType(name(), false, true, meta.getValue()));
            }
        }

        private NonDynamicFieldMapper(String simpleName, MappedFieldType mappedFieldType) {
            super(simpleName, mappedFieldType, MultiFields.empty(), CopyTo.empty());
        }

        @Override
        protected String contentType() {
            return NAME;
        }

        @Override
        protected void parseCreateField(DocumentParserContext context) throws IOException {}

        @Override
        public FieldMapper.Builder getMergeBuilder() {
            return new Builder(simpleName()).init(this);
        }
    }
}
