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
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.plugins.FieldPredicate;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    public void testPassthroughFieldDoesNotAddIntermediateParentAsObject() throws IOException {
        // Reproduce the scenario from https://github.com/elastic/elasticsearch/issues/144179:
        // A passthrough field (subobjects: false) with a sub-field whose name contains a dot,
        // e.g. "attributes.foo.bar". The intermediate segment "attributes.foo" must NOT be
        // synthesized as an implicit "object" field in the field capabilities response.
        MapperService mapperService = createMapperService("""
            { "_doc" : {
              "properties" : {
                "attributes" : {
                  "type" : "passthrough",
                  "priority" : 0,
                  "properties" : {
                    "foo.bar" : { "type" : "keyword" }
                  }
                }
              }
            } }
            """);
        SearchExecutionContext sec = createSearchExecutionContext(mapperService);

        Map<String, IndexFieldCapabilities> response = FieldCapabilitiesFetcher.retrieveFieldCaps(
            sec,
            s -> true,
            Strings.EMPTY_ARRAY,
            Strings.EMPTY_ARRAY,
            FieldPredicate.ACCEPT_ALL,
            getMockIndexShard(),
            true
        );

        // The leaf field should be present as keyword
        assertNotNull(response.get("attributes.foo.bar"));
        assertEquals("keyword", response.get("attributes.foo.bar").type());
        // The intermediate segment "attributes.foo" must NOT appear as an implicit object,
        // because the passthrough mapper enforces subobjects:false
        assertNull(
            "attributes.foo must not be synthesized as an implicit object under a subobjects:false passthrough mapper",
            response.get("attributes.foo")
        );
    }

    public void testSynthesizedObjectWithoutMapperIsNotPassthrough() throws IOException {
        // With subobjects:false at the root, "host.name" is a leaf and "host" is synthesized as an object without a backing
        // ObjectMapper. It must report the same passthrough status (false) as a real plain object, so that field caps of
        // such an index (e.g. logsdb) do not conflict with those of an index that maps "host" as a regular object.
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.field("subobjects", false);
            b.startObject("properties").startObject("host.name").field("type", "keyword").endObject().endObject();
        }));
        SearchExecutionContext sec = createSearchExecutionContext(mapperService);

        Map<String, IndexFieldCapabilities> response = FieldCapabilitiesFetcher.retrieveFieldCaps(
            sec,
            s -> true,
            Strings.EMPTY_ARRAY,
            Strings.EMPTY_ARRAY,
            FieldPredicate.ACCEPT_ALL,
            getMockIndexShard(),
            true
        );

        IndexFieldCapabilities host = response.get("host");
        assertNotNull(host);
        assertEquals("object", host.type());
        assertEquals(Boolean.FALSE, host.isPassthrough());
        assertNull(response.get("host.name").isPassthrough());
    }

    public void testAutoFlattenedPassthroughObjectIsFlagged() throws IOException {
        for (IndexMode indexMode : List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR)) {
            Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), indexMode.getName()).build();
            MapperService mapperService = createMapperService(settings, topMapping(b -> {
                b.field("subobjects", false);
                b.startObject("properties");
                b.startObject("resource.attributes").field("type", "passthrough").field("priority", 10);
                b.startObject("properties").startObject("host.name").field("type", "keyword").endObject().endObject();
                b.endObject();
                b.endObject();
            }));
            SearchExecutionContext sec = createSearchExecutionContext(mapperService);

            Map<String, IndexFieldCapabilities> response = FieldCapabilitiesFetcher.retrieveFieldCaps(
                sec,
                s -> true,
                Strings.EMPTY_ARRAY,
                Strings.EMPTY_ARRAY,
                FieldPredicate.ACCEPT_ALL,
                getMockIndexShard(),
                true
            );

            IndexFieldCapabilities attributes = response.get("resource.attributes");
            assertNotNull(attributes);
            assertEquals("object", attributes.type());
            assertEquals(Boolean.TRUE, attributes.isPassthrough());
            assertEquals(Boolean.FALSE, response.get("resource").isPassthrough());
            assertNull(response.get("resource.attributes.host.name").isPassthrough());
        }
    }

    public void testPassthroughObjectIsFlagged() throws IOException {
        // Passthrough sources keep their regular type ("object" / "flattened") but are additionally flagged as passthrough.
        // Plain objects and flattened fields, which could have been passthrough, are explicitly flagged as not passthrough,
        // while everything else (nested objects, leaf fields) carries no passthrough status at all.
        MapperService mapperService = createMapperService("""
            { "_doc" : {
              "properties" : {
                "attributes" : {
                  "type" : "passthrough",
                  "priority" : 10,
                  "properties" : {
                    "host.name" : { "type" : "keyword" }
                  }
                },
                "resource" : {
                  "properties" : {
                    "attributes" : {
                      "type" : "passthrough",
                      "priority" : 20,
                      "properties" : {
                        "host.name" : { "type" : "keyword" }
                      }
                    }
                  }
                },
                "plain" : {
                  "properties" : {
                    "field" : { "type" : "keyword" }
                  }
                },
                "nested" : {
                  "type" : "nested",
                  "properties" : {
                    "field" : { "type" : "keyword" }
                  }
                },
                "labels" : {
                  "type" : "flattened",
                  "passthrough" : { "priority" : 30 },
                  "properties" : {
                    "service.name" : { "type" : "keyword" }
                  }
                },
                "plain_flattened" : { "type" : "flattened" }
              }
            } }
            """);
        SearchExecutionContext sec = createSearchExecutionContext(mapperService);

        Map<String, IndexFieldCapabilities> response = FieldCapabilitiesFetcher.retrieveFieldCaps(
            sec,
            s -> true,
            Strings.EMPTY_ARRAY,
            Strings.EMPTY_ARRAY,
            FieldPredicate.ACCEPT_ALL,
            getMockIndexShard(),
            true
        );

        IndexFieldCapabilities attributes = response.get("attributes");
        assertNotNull(attributes);
        assertEquals("object", attributes.type());
        assertEquals(Boolean.TRUE, attributes.isPassthrough());

        IndexFieldCapabilities resourceAttributes = response.get("resource.attributes");
        assertNotNull(resourceAttributes);
        assertEquals("object", resourceAttributes.type());
        assertEquals(Boolean.TRUE, resourceAttributes.isPassthrough());

        // the parent of a passthrough object is a plain object
        IndexFieldCapabilities resource = response.get("resource");
        assertNotNull(resource);
        assertEquals("object", resource.type());
        assertEquals(Boolean.FALSE, resource.isPassthrough());

        IndexFieldCapabilities plain = response.get("plain");
        assertNotNull(plain);
        assertEquals("object", plain.type());
        assertEquals(Boolean.FALSE, plain.isPassthrough());

        // nested objects cannot be passthrough, so they carry no status
        IndexFieldCapabilities nested = response.get("nested");
        assertNotNull(nested);
        assertEquals("nested", nested.type());
        assertNull(nested.isPassthrough());

        // flattened fields are passthrough sources when configured as such
        IndexFieldCapabilities labels = response.get("labels");
        assertNotNull(labels);
        assertEquals("flattened", labels.type());
        assertEquals(Boolean.TRUE, labels.isPassthrough());

        IndexFieldCapabilities plainFlattened = response.get("plain_flattened");
        assertNotNull(plainFlattened);
        assertEquals("flattened", plainFlattened.type());
        assertEquals(Boolean.FALSE, plainFlattened.isPassthrough());

        // leaf fields, including passthrough sub-fields and their root-level aliases, carry no passthrough status
        assertNotNull(response.get("attributes.host.name"));
        assertNull(response.get("attributes.host.name").isPassthrough());
        assertNotNull(response.get("host.name"));
        assertNull(response.get("host.name").isPassthrough());
        assertNotNull(response.get("labels.service.name"));
        assertNull(response.get("labels.service.name").isPassthrough());
        assertNotNull(response.get("service.name"));
        assertNull(response.get("service.name").isPassthrough());
        // intermediate segments of dotted sub-field names must not be synthesized as objects
        assertNull(response.get("attributes.host"));
        assertNull(response.get("labels.service"));
    }

    public void testIndexLocalAnalyzerNameIsDropped() throws IOException {
        // The same mapping in two indices. The built-in name "default" is reported with the mapping's
        // position_increment_gap, until index.analysis redefines it and field-caps must withhold it.
        String mapping = """
            { "_doc" : {
              "properties" : {
                "body" : { "type" : "text", "position_increment_gap" : 7 }
              }
            } }
            """;
        Settings redefinesDefault = Settings.builder()
            .put("index.analysis.analyzer.default.type", "custom")
            .put("index.analysis.analyzer.default.tokenizer", "standard")
            .build();

        IndexFieldCapabilities reported = retrieveBodyFieldCaps(Settings.EMPTY, mapping);
        assertEquals("default", reported.indexAnalyzer());
        assertEquals(7, reported.indexAnalyzerPositionIncrementGap());
        assertFalse(reported.indexLocalAnalyzer());

        IndexFieldCapabilities withheld = retrieveBodyFieldCaps(redefinesDefault, mapping);
        assertNull(withheld.indexAnalyzer());
        assertTrue(withheld.indexLocalAnalyzer());
    }

    private IndexFieldCapabilities retrieveBodyFieldCaps(Settings settings, String mapping) throws IOException {
        SearchExecutionContext sec = createSearchExecutionContext(createMapperService(settings, mapping));
        return FieldCapabilitiesFetcher.retrieveFieldCaps(
            sec,
            s -> true,
            Strings.EMPTY_ARRAY,
            Strings.EMPTY_ARRAY,
            FieldPredicate.ACCEPT_ALL,
            getMockIndexShard(),
            true
        ).get("body");
    }

    public void testAnalyzerNamesDigest() {
        // Both sets print as [standard, x, y], yet only the first withholds "standard".
        assertNotEquals(
            FieldCapabilitiesFetcher.analyzerNamesDigest(Set.of("standard", "x, y")),
            FieldCapabilitiesFetcher.analyzerNamesDigest(Set.of("standard, x", "y"))
        );
        assertEquals(
            FieldCapabilitiesFetcher.analyzerNamesDigest(new LinkedHashSet<>(List.of("a", "b"))),
            FieldCapabilitiesFetcher.analyzerNamesDigest(new LinkedHashSet<>(List.of("b", "a")))
        );
        // Non-empty without configured analyzers, so the dedup hash never equals one from an older node.
        assertFalse(FieldCapabilitiesFetcher.analyzerNamesDigest(Set.of()).isEmpty());
    }

    private IndexShard getMockIndexShard() {
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.getFieldInfos()).thenReturn(FieldInfos.EMPTY);
        return indexShard;
    }

}
