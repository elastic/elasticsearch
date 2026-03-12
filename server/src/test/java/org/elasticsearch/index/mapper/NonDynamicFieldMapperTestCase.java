/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequestBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public abstract class NonDynamicFieldMapperTestCase extends ESSingleNodeTestCase {

    protected abstract String getTypeName();

    protected abstract String getMapping();

    public void testCreateExplicitMappingSucceeds() throws Exception {
        String mapping = String.format(Locale.ROOT, """
            {
              "_doc": {
                "properties": {
                  "field": {
                    %s
                  }
                }
              }
            }
            """, getMapping());
        var resp = client().admin().indices().prepareCreate("test").setMapping(mapping).get();
        assertTrue(resp.isAcknowledged());
        var mappingsResp = client().admin().indices().prepareGetMappings(TEST_REQUEST_TIMEOUT, "test").get();
        var mappingMetadata = mappingsResp.getMappings().get("test");
        var fieldType = XContentMapValues.extractValue("properties.field.type", mappingMetadata.getSourceAsMap());
        assertThat(fieldType, equalTo(getTypeName()));
    }

    public void testCreateDynamicMappingFails() throws Exception {
        String mapping = String.format(Locale.ROOT, """
            {
              "_doc": {
                  "dynamic_templates": [
                    {
                      "strings_as_type": {
                        "match_mapping_type": "string",
                        "mapping": {
                          %s
                        }
                      }
                    }
                  ]
                }
            }
            """, getMapping());
        CreateIndexRequestBuilder req = client().admin().indices().prepareCreate("test").setMapping(mapping);
        Exception exc = expectThrows(Exception.class, () -> req.get());
        assertThat(exc.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(exc.getCause().getCause(), instanceOf(MapperParsingException.class));
        assertThat(exc.getCause().getCause().getMessage(), containsString("[" + getTypeName() + "] can't be used in dynamic templates"));
    }

    public void testUpdateDynamicMappingFails() throws Exception {
        var resp = client().admin().indices().prepareCreate("test").get();
        assertTrue(resp.isAcknowledged());
        String mapping = String.format(Locale.ROOT, """
            {
              "_doc": {
                  "dynamic_templates": [
                    {
                      "strings_as_type": {
                        "match_mapping_type": "string",
                        "mapping": {
                            %s
                        }
                      }
                    }
                  ]
                }
            }
            """, getMapping());
        var req = client().admin().indices().preparePutMapping("test").setSource(mapping, XContentType.JSON);
        Exception exc = expectThrows(Exception.class, () -> req.get());
        assertThat(exc.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(exc.getCause().getCause(), instanceOf(MapperParsingException.class));
        assertThat(exc.getCause().getCause().getMessage(), containsString("[" + getTypeName() + "] can't be used in dynamic templates"));
    }

    public void testCreateDynamicMappingInIndexTemplateFails() throws Exception {
        String mapping = String.format(Locale.ROOT, """
            {
              "_doc": {
                "dynamic_templates": [
                  {
                    "strings_as_type": {
                      "match_mapping_type": "string",
                      "mapping": {
                        %s
                      }
                    }
                  }
                ]
              }
            }
            """, getMapping());
        PutIndexTemplateRequestBuilder req = client().admin()
            .indices()
            .preparePutTemplate("template1")
            .setMapping(mapping, XContentType.JSON)
            .setPatterns(List.of("test*"));
        Exception exc = expectThrows(Exception.class, () -> req.get());
        assertThat(exc.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(exc.getCause().getCause(), instanceOf(MapperParsingException.class));
        assertThat(exc.getCause().getCause().getMessage(), containsString("[" + getTypeName() + "] can't be used in dynamic templates"));
    }

    public void testCreateExplicitMappingInIndexTemplateSucceeds() throws Exception {
        String mapping = String.format(Locale.ROOT, """
            {
              "_doc": {
                "properties": {
                  "field": {
                    %s
                  }
                }
              }
            }
            """, getMapping());
        PutIndexTemplateRequestBuilder req = client().admin()
            .indices()
            .preparePutTemplate("template1")
            .setMapping(mapping, XContentType.JSON)
            .setPatterns(List.of("test*"));
        assertTrue(req.get().isAcknowledged());

        var resp = client().prepareIndex("test1").setSource("field", "hello world").get();
        assertThat(resp.status(), equalTo(RestStatus.CREATED));

        var mappingsResp = client().admin().indices().prepareGetMappings(TEST_REQUEST_TIMEOUT, "test1").get();
        var mappingMetadata = mappingsResp.getMappings().get("test1");
        var fieldType = XContentMapValues.extractValue("properties.field.type", mappingMetadata.getSourceAsMap());
        assertThat(fieldType, equalTo(getTypeName()));
    }
}
