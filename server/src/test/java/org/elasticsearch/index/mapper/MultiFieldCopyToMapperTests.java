/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */


package org.elasticsearch.index.mapper;

import static org.hamcrest.core.IsEqual.equalTo;

public class MultiFieldCopyToMapperTests extends MapperServiceTestCase {

    public void testExceptionForCopyToInMultiFields() {

        Exception e = expectThrows(IllegalArgumentException.class, () -> createMapperService(mapping(b -> {
            b.startObject("a").field("type", "text").endObject();
            b.startObject("b");
            {
                b.field("type", "text");
                b.startObject("fields");
                {
                    b.startObject("subfield");
                    {
                        b.field("type", "text");
                        b.field("copy_to", "a");
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        })));

        assertThat(e.getMessage(), equalTo("[copy_to] may not be used to copy from a multi-field: [b.subfield]"));
    }

}
