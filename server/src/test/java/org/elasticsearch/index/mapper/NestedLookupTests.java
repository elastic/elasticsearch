/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.index.IndexVersion;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;

public class NestedLookupTests extends MapperServiceTestCase {

    public void testMultiLevelParents() throws IOException {

        String mapping = "{\n"
            + "  \"_doc\": {\n"
            + "    \"properties\" : {\n"
            + "      \"SWufZ\" : {\n"
            + "        \"type\" : \"nested\",\n"
            + "        \"properties\" : {\n"
            + "          \"ZCPoX\" : {\n"
            + "            \"type\" : \"keyword\"\n"
            + "          },\n"
            + "          \"NnUDX\" : {\n"
            + "            \"properties\" : {\n"
            + "              \"dljyS\" : {\n"
            + "                \"type\" : \"nested\",\n"
            + "                \"properties\" : {\n"
            + "                  \"JYmZZ\" : {\n"
            + "                    \"type\" : \"keyword\"\n"
            + "                  },\n"
            + "                  \"EvbGO\" : {\n"
            + "                    \"type\" : \"nested\",\n"
            + "                    \"properties\" : {\n"
            + "                      \"LAgoT\" : {\n"
            + "                        \"type\" : \"keyword\"\n"
            + "                      }\n"
            + "                    }\n"
            + "                  }\n"
            + "                }\n"
            + "              }\n"
            + "            }\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}";

        MapperService mapperService = createMapperService(mapping);

        NestedLookup lookup = mapperService.mappingLookup().nestedLookup();
        assertEquals("SWufZ.NnUDX.dljyS", lookup.getNestedParent("SWufZ.NnUDX.dljyS.EvbGO"));
        assertThat(lookup.getNestedParentFilters().keySet(), hasSize(2));

    }

    private static NestedObjectMapper buildMapper(String name) {
        return new NestedObjectMapper.Builder(name, IndexVersion.current()).build(MapperBuilderContext.root(false));
    }

    public void testAllParentFilters() {
        List<NestedObjectMapper> mappers = List.of(
            buildMapper("a.b"),
            buildMapper("a.d"),
            buildMapper("a.b.c.d.e"),
            buildMapper("a.b.d"),
            buildMapper("a"),
            buildMapper("a.b.c.d")
        );

        NestedLookup lookup = NestedLookup.build(mappers);
        assertThat(lookup.getNestedParentFilters().keySet(), containsInAnyOrder("a", "a.b", "a.b.c.d"));
    }

    public void testGetNestedParent() {
        List<NestedObjectMapper> mappers = List.of(
            buildMapper("a.b"),
            buildMapper("a.d"),
            buildMapper("a.b.c.d.e"),
            buildMapper("a.b.d"),
            buildMapper("a"),
            buildMapper("a.b.c.d")
        );

        NestedLookup lookup = NestedLookup.build(mappers);
        assertNull(lookup.getNestedParent("foo"));
        assertEquals("a.b", lookup.getNestedParent("a.b.foo"));
        assertEquals("a", lookup.getNestedParent("a.foo.bar"));
        assertEquals("a.b.d", lookup.getNestedParent("a.b.d.foo"));
        assertEquals("a.b", lookup.getNestedParent("a.b.c.foo"));
        assertNull(lookup.getNestedParent("aa.b"));
    }

    public void testGetImmediateChildren() {
        List<NestedObjectMapper> mappers = List.of(
            buildMapper("a.b"),
            buildMapper("a.d"),
            buildMapper("a.b.c.d.e"),
            buildMapper("a.b.d"),
            buildMapper("a"),
            buildMapper("a.b.c.d")
        );

        NestedLookup lookup = NestedLookup.build(mappers);
        assertEquals(List.of("a.b.c.d", "a.b.d"), lookup.getImmediateChildMappers("a.b"));
        assertEquals(List.of(), lookup.getImmediateChildMappers("a.b.d"));
        assertEquals(List.of("a"), lookup.getImmediateChildMappers(""));
        assertEquals(List.of(), lookup.getImmediateChildMappers("aa"));
        assertEquals(List.of(), lookup.getImmediateChildMappers("a.c"));
    }

}
