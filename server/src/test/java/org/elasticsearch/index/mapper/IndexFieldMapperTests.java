/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class IndexFieldMapperTests extends MapperServiceTestCase {

    public void testDefaultDisabledIndexMapper() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {}));
        ParsedDocument doc = docMapper.parse(source(b -> b.field("field", "value")));
        assertThat(doc.rootDoc().get("_index"), nullValue());
        assertThat(doc.rootDoc().get("field"), equalTo("value"));
    }

    public void testIndexNotConfigurable() {
        MapperParsingException e = expectThrows(MapperParsingException.class,
                () -> createMapperService(topMapping(b -> b.startObject("_index").endObject())));
        assertThat(e.getMessage(), containsString("_index is not configurable"));
    }

}
