/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fieldcaps;

import org.apache.lucene.index.FieldInfos;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.plugins.FieldPredicate;

import java.io.IOException;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FieldCapabilitiesFilterTests extends MapperServiceTestCase {

    public void testExcludeNestedFields() throws IOException {
        MapperService mapperService = createMapperService("""
            { "_doc" : {
              "properties" : {
                "field1" : { "type" : "keyword" },
                "field2" : {
                  "type" : "nested",
                  "properties" : {
                    "field3" : { "type" : "keyword" }
                  }
                },
                "field4" : { "type" : "keyword" }
              }
            } }
            """);
        SearchExecutionContext sec = createSearchExecutionContext(mapperService);

        Map<String, IndexFieldCapabilities> response = FieldCapabilitiesFetcher.retrieveFieldCaps(
            sec,
            s -> true,
            new String[] { "-nested" },
            Strings.EMPTY_ARRAY,
            FieldPredicate.ACCEPT_ALL,
            getMockIndexShard(),
            true
        );

        assertNotNull(response.get("field1"));
        assertNotNull(response.get("field4"));
        assertNull(response.get("field2"));
        assertNull(response.get("field2.field3"));
    }

    public void testMetadataFilters() throws IOException {
        MapperService mapperService = createMapperService("""
            { "_doc" : {
              "properties" : {
                "field1" : { "type" : "keyword" },
                "field2" : { "type" : "keyword" }
              }
            } }
            """);
        SearchExecutionContext sec = createSearchExecutionContext(mapperService);

        {
            Map<String, IndexFieldCapabilities> response = FieldCapabilitiesFetcher.retrieveFieldCaps(
                sec,
                s -> true,
                new String[] { "+metadata" },
                Strings.EMPTY_ARRAY,
                FieldPredicate.ACCEPT_ALL,
                getMockIndexShard(),
                true
            );
            assertNotNull(response.get("_index"));
            assertNull(response.get("field1"));
        }
        {
            Map<String, IndexFieldCapabilities> response = FieldCapabilitiesFetcher.retrieveFieldCaps(
                sec,
                s -> true,
                new String[] { "-metadata" },
                Strings.EMPTY_ARRAY,
                FieldPredicate.ACCEPT_ALL,
                getMockIndexShard(),
                true
            );
            assertNull(response.get("_index"));
            assertNotNull(response.get("field1"));
        }
    }

    public void testDimensionFilters() throws IOException {
        MapperService mapperService = createMapperService(
            Settings.builder().put("index.mode", "time_series").put("index.routing_path", "dim.*").build(),
            """
                { "_doc" : {
                  "properties" : {
                    "metric" : { "type" : "long" },
                    "dimension_1" : { "type" : "keyword", "time_series_dimension" : "true" },
                    "dimension_2" : { "type" : "long", "time_series_dimension" : "true" }
                  }
                } }
                """
        );
        SearchExecutionContext sec = createSearchExecutionContext(mapperService);

        {
            // First, test without the filter
            Map<String, IndexFieldCapabilities> response = FieldCapabilitiesFetcher.retrieveFieldCaps(
                sec,
                s -> s.equals("metric"),
                Strings.EMPTY_ARRAY,
                Strings.EMPTY_ARRAY,
                FieldPredicate.ACCEPT_ALL,
                getMockIndexShard(),
                true
            );
            assertNotNull(response.get("metric"));
            assertNull(response.get("dimension_1"));
            assertNull(response.get("dimension_2"));
        }

        {
            // then, test with the filter
            Map<String, IndexFieldCapabilities> response = FieldCapabilitiesFetcher.retrieveFieldCaps(
                sec,
                s -> s.equals("metric"),
                new String[] { "+dimension" },
                Strings.EMPTY_ARRAY,
                FieldPredicate.ACCEPT_ALL,
                getMockIndexShard(),
                true
            );
            assertNotNull(response.get("dimension_1"));
            assertNotNull(response.get("dimension_2"));
            assertNotNull(response.get("metric"));
        }
    }

    public void testExcludeMultifields() throws IOException {
        MapperService mapperService = createMapperService("""
            { "_doc" : {
              "properties" : {
                "field1" : {
                  "type" : "text",
                  "fields" : {
                    "keyword" : { "type" : "keyword" }
                  }
                },
                "field2" : { "type" : "keyword" }
              },
              "runtime" : {
                "field2.keyword" : { "type" : "keyword" }
              }
            } }
            """);
        SearchExecutionContext sec = createSearchExecutionContext(mapperService);

        Map<String, IndexFieldCapabilities> response = FieldCapabilitiesFetcher.retrieveFieldCaps(
            sec,
            s -> true,
            new String[] { "-multifield" },
            Strings.EMPTY_ARRAY,
            FieldPredicate.ACCEPT_ALL,
            getMockIndexShard(),
            true
        );
        assertNotNull(response.get("field1"));
        assertNull(response.get("field1.keyword"));
        assertNotNull(response.get("field2"));
        assertNotNull(response.get("field2.keyword"));
        assertNotNull(response.get("_index"));
    }

    public void testDontIncludeParentInfo() throws IOException {
        MapperService mapperService = createMapperService("""
            { "_doc" : {
              "properties" : {
                "parent" : {
                  "properties" : {
                    "field1" : { "type" : "keyword" },
                    "field2" : { "type" : "keyword" }
                  }
                }
              }
            } }
            """);
        SearchExecutionContext sec = createSearchExecutionContext(mapperService);

        Map<String, IndexFieldCapabilities> response = FieldCapabilitiesFetcher.retrieveFieldCaps(
            sec,
            s -> true,
            new String[] { "-parent" },
            Strings.EMPTY_ARRAY,
            FieldPredicate.ACCEPT_ALL,
            getMockIndexShard(),
            true
        );
        assertNotNull(response.get("parent.field1"));
        assertNotNull(response.get("parent.field2"));
        assertNull(response.get("parent"));
    }

    public void testSecurityFilter() throws IOException {
        MapperService mapperService = createMapperService("""
            { "_doc" : {
              "properties" : {
                "permitted1" : { "type" : "keyword" },
                "permitted2" : { "type" : "keyword" },
                "forbidden" : { "type" : "keyword" }
              }
            } }
            """);
        SearchExecutionContext sec = createSearchExecutionContext(mapperService);
        FieldPredicate securityFilter = new FieldPredicate() {
            @Override
            public boolean test(String field) {
                return field.startsWith("permitted");
            }

            @Override
            public String modifyHash(String hash) {
                return "only-permitted:" + hash;
            }

            @Override
            public long ramBytesUsed() {
                return 0;
            }
        };

        {
            Map<String, IndexFieldCapabilities> response = FieldCapabilitiesFetcher.retrieveFieldCaps(
                sec,
                s -> true,
                Strings.EMPTY_ARRAY,
                Strings.EMPTY_ARRAY,
                securityFilter,
                getMockIndexShard(),
                true
            );

            assertNotNull(response.get("permitted1"));
            assertNull(response.get("forbidden"));
            assertNotNull(response.get("_index"));     // security filter doesn't apply to metadata
        }

        {
            Map<String, IndexFieldCapabilities> response = FieldCapabilitiesFetcher.retrieveFieldCaps(
                sec,
                s -> true,
                new String[] { "-metadata" },
                Strings.EMPTY_ARRAY,
                securityFilter,
                getMockIndexShard(),
                true
            );

            assertNotNull(response.get("permitted1"));
            assertNull(response.get("forbidden"));
            assertNull(response.get("_index"));     // -metadata filter applies on top
        }
    }

    public void testFieldTypeFiltering() throws IOException {
        MapperService mapperService = createMapperService("""
            { "_doc" : {
              "properties" : {
                "field1" : { "type" : "keyword" },
                "field2" : { "type" : "long" },
                "field3" : { "type" : "text" }
              }
            } }
            """);
        SearchExecutionContext sec = createSearchExecutionContext(mapperService);

        Map<String, IndexFieldCapabilities> response = FieldCapabilitiesFetcher.retrieveFieldCaps(
            sec,
            s -> true,
            Strings.EMPTY_ARRAY,
            new String[] { "text", "keyword" },
            FieldPredicate.ACCEPT_ALL,
            getMockIndexShard(),
            true
        );
        assertNotNull(response.get("field1"));
        assertNull(response.get("field2"));
        assertNotNull(response.get("field3"));
        assertNull(response.get("_index"));
    }

    private IndexShard getMockIndexShard() {
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.getFieldInfos()).thenReturn(FieldInfos.EMPTY);
        return indexShard;
    }

}
