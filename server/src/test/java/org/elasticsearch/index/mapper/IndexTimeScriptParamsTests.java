/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.hamcrest.Matchers;

import java.io.IOException;

public class IndexTimeScriptParamsTests extends MapperServiceTestCase {

    public void testDocAccess() throws IOException {

        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("long_field").field("type", "long").endObject();
            b.startObject("double_field").field("type", "double").endObject();
            b.startObject("string_field").field("type", "keyword").endObject();
        }));

        SourceToParse source = source(b -> {
            b.array("long_field", 10, "15", 20);
            b.array("double_field", "3.14", 2.78);
            b.array("string_field", "alice", "bob");
        });
        ParsedDocument doc = mapperService.documentMapper().parse(source);

        IndexTimeScriptParams params = new IndexTimeScriptParams(source.source(), doc.rootDoc(), mapperService::fieldType);

        assertThat(params.source().extractRawValues("string_field"),
            Matchers.containsInAnyOrder("alice", "bob"));
        assertThat(params.doc().get("double_field"),
            Matchers.containsInAnyOrder(3.14, 2.78));
        assertThat(params.doc().get("long_field"),
            Matchers.containsInAnyOrder(10L, 15L, 20L));
    }

}
