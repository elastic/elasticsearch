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
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.MappingBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;

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

    public void testRichMappingAgrees() throws IOException {
        Set<String> keys = assertAgrees(RICH_MAPPING);
        // the shapes the builders have to get right are really in the mapping this checks
        assertThat(
            keys,
            hasItems(
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
                "flat.known",
                "_routing",
                "_source"
            )
        );
    }

    public void testSubobjectsFalseAgrees() throws IOException {
        MapperService mapperService = createMapperService(mappingNoSubobjects(b -> {
            b.startObject("a.b").field("type", "keyword");
            b.startObject("fields").startObject("raw").field("type", "keyword").endObject().endObject();
            b.endObject();
            b.startObject("o").startObject("properties").startObject("x").field("type", "long").endObject().endObject().endObject();
            b.startObject("n").field("type", "nested");
            b.startObject("properties").startObject("y").field("type", "keyword").endObject().endObject();
            b.endObject();
        }));
        Set<String> keys = assertAgrees(mapperService);
        assertThat(keys, hasItems("a.b", "a.b.raw", "o.x", "n.y"));
    }

    public void testEmptyMappingAgrees() throws IOException {
        assertAgrees(createMapperService(mapping(b -> {})));
    }

    /**
     * @return the full paths the mappers listed for a request for everything
     */
    private Set<String> assertAgrees(String mapping) throws IOException {
        return assertAgrees(createMapperService(mapping));
    }

    private static MappingBuilder parseMappings(MapperService mapperService, DocumentMapper documentMapper) {
        return mapperService.parseMappings(documentMapper.mappingSource(), MapperService.MergeReason.MAPPING_RECOVERY);
    }

    private Set<String> assertAgrees(MapperService mapperService) {
        DocumentMapper documentMapper = mapperService.documentMapper();
        Predicate<String> everything = field -> true;
        Set<String> everythingMapped = null;
        for (boolean includeDefaults : new boolean[] { false, true }) {
            for (String[] fields : REQUESTS) {
                GetFieldMappingsIndexRequest request = new GetFieldMappingsIndexRequest(
                    new GetFieldMappingsRequest().fields(fields).includeDefaults(includeDefaults),
                    "index"
                );
                Map<String, FieldMappingMetadata> fromBuilders = TransportGetFieldMappingsIndexAction.findFieldMappings(
                    everything,
                    parseMappings(mapperService, documentMapper),
                    request
                );
                Map<String, FieldMappingMetadata> fromMappers = TransportGetFieldMappingsIndexAction.findFieldMappingsFromMappers(
                    everything,
                    documentMapper.mappers(),
                    request
                );
                assertThat(
                    "fields " + String.join(",", fields) + " include_defaults " + includeDefaults,
                    fromBuilders,
                    equalTo(fromMappers)
                );
                if (includeDefaults == false && fields.length == 1 && fields[0].equals("*")) {
                    everythingMapped = fromMappers.keySet();
                }
            }
        }
        return everythingMapped;
    }
}
