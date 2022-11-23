/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class IpRangeFieldTypeTests extends FieldTypeTestCase {

    public void testFetchSourceValue() throws IOException {
        RangeFieldMapper mapper = new RangeFieldMapper.Builder("field", RangeType.IP, true).build(MapperBuilderContext.root(false));
        Map<String, Object> range = Map.of("gte", "2001:db8:0:0:0:0:2:1");
        assertEquals(List.of(Map.of("gte", "2001:db8::2:1")), fetchSourceValue(mapper.fieldType(), range));
        assertEquals(List.of("2001:db8::2:1/32"), fetchSourceValue(mapper.fieldType(), "2001:db8:0:0:0:0:2:1/32"));
    }
}
