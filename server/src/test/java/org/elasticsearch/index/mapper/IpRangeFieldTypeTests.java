/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.Version;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class IpRangeFieldTypeTests extends FieldTypeTestCase {

    public void testFetchSourceValue() throws IOException {
        RangeFieldMapper mapper = new RangeFieldMapper.Builder("field", RangeType.IP, false, Version.CURRENT).build(
            MapperBuilderContext.ROOT
        );
        Map<String, Object> range = Collections.singletonMap("gte", "2001:db8:0:0:0:0:2:1");
        assertEquals(
            Collections.singletonList(Collections.singletonMap("gte", "2001:db8::2:1")),
            fetchSourceValue(mapper.fieldType(), range)
        );
        assertEquals(Collections.singletonList("2001:db8::2:1/32"), fetchSourceValue(mapper.fieldType(), "2001:db8:0:0:0:0:2:1/32"));
    }
}
