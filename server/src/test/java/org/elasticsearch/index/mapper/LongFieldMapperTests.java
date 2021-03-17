/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.NumberFieldTypeTests.OutOfRangeSpec;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

import static org.hamcrest.Matchers.arrayWithSize;

public class LongFieldMapperTests extends WholeNumberFieldMapperTests {

    @Override
    protected Number missingValue() {
        return 123L;
    }

    @Override
    protected List<OutOfRangeSpec> outOfRangeSpecs() {
        return List.of(
            OutOfRangeSpec.of(NumberFieldMapper.NumberType.LONG, "9223372036854775808", "out of range for a long"),
            OutOfRangeSpec.of(NumberFieldMapper.NumberType.LONG, "1e999999999", "out of range for a long"),
            OutOfRangeSpec.of(NumberFieldMapper.NumberType.LONG, "-9223372036854775809", "out of range for a long"),
            OutOfRangeSpec.of(NumberFieldMapper.NumberType.LONG, "-1e999999999", "out of range for a long"),
            OutOfRangeSpec.of(NumberFieldMapper.NumberType.LONG, new BigInteger("9223372036854775808"), "out of range of long"),
            OutOfRangeSpec.of(NumberFieldMapper.NumberType.LONG, new BigInteger("-9223372036854775809"), "out of range of long")
        );
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "long");
    }

    public void testLongIndexingOutOfRange() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "long").field("ignore_malformed", true)));
        ParsedDocument doc = mapper.parse(
            source(b -> b.rawField("field", new BytesArray("9223372036854775808").streamInput(), XContentType.JSON))
        );
        assertEquals(0, doc.rootDoc().getFields("field").length);
    }

    public void testLongIndexingCoercesIntoRange() throws Exception {
        // the following two strings are in-range for a long after coercion
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "9223372036854775807.9")));
        assertThat(doc.rootDoc().getFields("field"), arrayWithSize(2));
        doc = mapper.parse(source(b -> b.field("field", "-9223372036854775808.9")));
        assertThat(doc.rootDoc().getFields("field"), arrayWithSize(2));
    }
}
