/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.mapping.get;

import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetadata;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.MappingBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;

public class GetFieldMappingsFromBuildersTests extends MapperServiceTestCase {

    private static final String[][] REQUESTS = {
        { "*" },
        { "kw" },
        { "kw.raw", "nest.obj.deep", "alias_kw", "missing" },
        { "nest.*" },
        { "*.raw" },
        { "_*" },
        { "kw", "kw.*", "txt*" } };

    private static final String RICH_MAPPING = """
        {
          "_doc": {
            "_source": { "mode": "synthetic" },
            "properties": {
              "kw": {
                "type": "keyword",
                "fields": {
                  "raw": { "type": "keyword", "ignore_above": 10 },
                  "txt": { "type": "text" }
                }
              },
              "txt": { "type": "text", "copy_to": [ "kw" ], "fields": { "sub": { "type": "keyword" } } },
              "obj": {
                "properties": {
                  "a": { "type": "long" },
                  "b": { "type": "keyword", "fields": { "f": { "type": "keyword" } } }
                }
              },
              "nest": {
                "type": "nested",
                "properties": {
                  "n": { "type": "keyword" },
                  "obj": { "properties": { "deep": { "type": "date" } } }
                }
              },
              "alias_kw": { "type": "alias", "path": "kw" },
              "flat": { "type": "flattened", "properties": { "known": { "type": "keyword" } } }
            }
          }
        }
        """;

    /** Every path the rich mapping declares, leaving out the metadata fields, which only some of the tests look at. */
    private static final Set<String> RICH_MAPPING_PATHS = Set.of(
        "kw",
        "kw.raw",
        "kw.txt",
        "txt",
        "txt.sub",
        "obj.a",
        "obj.b",
        "obj.b.f",
        "nest.n",
        "nest.obj.deep",
        "alias_kw",
        "flat",
        "flat.known"
    );

    public void testRichMappingReturnsEveryDeclaredPath() throws IOException {
        MapperService mapperService = createMapperService(RICH_MAPPING);
        Set<String> keys = find(mapperService, false, "*").keySet();
        assertThat(keys, hasItems(RICH_MAPPING_PATHS.toArray(String[]::new)));
        assertThat(keys, hasItems("_routing", "_source"));
        // objects, including the nested one, are not fields of their own
        assertThat(keys, not(hasItems("obj", "nest")));
    }

    public void testRequestedFieldsAreMatchedAgainstFullPaths() throws IOException {
        MapperService mapperService = createMapperService(RICH_MAPPING);
        for (boolean includeDefaults : new boolean[] { false, true }) {
            for (String[] fields : REQUESTS) {
                Set<String> expected = new TreeSet<>();
                for (String path : RICH_MAPPING_PATHS) {
                    if (Regex.simpleMatch(fields, path)) {
                        expected.add(path);
                    }
                }
                Set<String> actual = new TreeSet<>(find(mapperService, includeDefaults, fields).keySet());
                // metadata fields are returned for the patterns that match them, and the paths above are the only others
                actual.removeIf(path -> path.startsWith("_"));
                assertThat("fields " + String.join(",", fields) + " include_defaults " + includeDefaults, actual, equalTo(expected));
            }
        }
    }

    public void testFieldsAreWrittenUnderTheirLeafName() throws IOException {
        MapperService mapperService = createMapperService(RICH_MAPPING);
        Map<String, FieldMappingMetadata> found = find(mapperService, false, "kw.raw", "nest.obj.deep", "alias_kw", "obj.b");
        assertThat(found.keySet(), equalTo(Set.of("kw.raw", "nest.obj.deep", "alias_kw", "obj.b")));
        assertThat(source(found, "kw.raw"), equalTo("{\"raw\":{\"type\":\"keyword\",\"ignore_above\":10}}"));
        assertThat(source(found, "nest.obj.deep"), equalTo("{\"deep\":{\"type\":\"date\"}}"));
        assertThat(source(found, "alias_kw"), equalTo("{\"alias_kw\":{\"type\":\"alias\",\"path\":\"kw\"}}"));
        // a field's own multi-fields are part of it
        assertThat(source(found, "obj.b"), equalTo("{\"b\":{\"type\":\"keyword\",\"fields\":{\"f\":{\"type\":\"keyword\"}}}}"));
    }

    public void testCopyToIsSerialized() throws IOException {
        MapperService mapperService = createMapperService(RICH_MAPPING);
        assertThat(
            source(find(mapperService, false, "txt"), "txt"),
            equalTo("{\"txt\":{\"type\":\"text\",\"fields\":{\"sub\":{\"type\":\"keyword\"}},\"copy_to\":[\"kw\"]}}")
        );
    }

    public void testIncludeDefaultsAddsParametersToTheSameField() throws IOException {
        MapperService mapperService = createMapperService(RICH_MAPPING);
        Map<String, Object> without = find(mapperService, false, "obj.a").get("obj.a").sourceAsMap();
        Map<String, Object> with = find(mapperService, true, "obj.a").get("obj.a").sourceAsMap();
        @SuppressWarnings("unchecked")
        Map<String, Object> withoutBody = (Map<String, Object>) without.get("a");
        @SuppressWarnings("unchecked")
        Map<String, Object> withBody = (Map<String, Object>) with.get("a");
        assertThat(withoutBody, equalTo(Map.of("type", "long")));
        assertThat(withBody.keySet(), hasItems("type", "index", "doc_values", "coerce", "ignore_malformed"));
        assertThat(withBody.get("type"), equalTo("long"));
    }

    public void testSubobjectsFalse() throws IOException {
        MapperService mapperService = createMapperService(mappingNoSubobjects(b -> {
            b.startObject("a.b").field("type", "keyword");
            b.startObject("fields").startObject("raw").field("type", "keyword").endObject().endObject();
            b.endObject();
            b.startObject("o").startObject("properties").startObject("x").field("type", "long").endObject().endObject().endObject();
            b.startObject("n").field("type", "nested");
            b.startObject("properties").startObject("y").field("type", "keyword").endObject().endObject();
            b.endObject();
        }));
        Map<String, FieldMappingMetadata> found = find(mapperService, false, "*");
        assertThat(found.keySet(), hasItems("a.b", "a.b.raw", "o.x", "n.y"));
        // with subobjects disabled a dotted field is a leaf of the root and is written under its whole dotted name
        assertThat(source(found, "a.b"), equalTo("{\"a.b\":{\"type\":\"keyword\",\"fields\":{\"raw\":{\"type\":\"keyword\"}}}}"));
    }

    public void testEmptyMapping() throws IOException {
        Map<String, FieldMappingMetadata> found = find(createMapperService(mapping(b -> {})), false, "*");
        assertThat(found.keySet().stream().filter(path -> path.startsWith("_") == false).toList(), equalTo(java.util.List.of()));
    }

    /** The fields filter lets everything through, as it does for a user with access to the whole index. */
    private static Map<String, FieldMappingMetadata> find(MapperService mapperService, boolean includeDefaults, String... fields) {
        DocumentMapper documentMapper = mapperService.documentMapper();
        Predicate<String> everything = field -> true;
        GetFieldMappingsIndexRequest request = new GetFieldMappingsIndexRequest(
            new GetFieldMappingsRequest().fields(fields).includeDefaults(includeDefaults),
            "index"
        );
        MappingBuilder mappingBuilder = mapperService.parseMappings(
            documentMapper.mappingSource(),
            MapperService.MergeReason.MAPPING_RECOVERY
        );
        return TransportGetFieldMappingsIndexAction.findFieldMappings(everything, mappingBuilder, request);
    }

    private static String source(Map<String, FieldMappingMetadata> found, String path) {
        return found.get(path).source().utf8ToString();
    }
}
