/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.index.mapper.FieldArrayContext.parseOffsetArray;

public class FieldArrayContextTests extends ESTestCase {

    public void testOffsets() throws IOException {
        var context = new FieldArrayContext();
        context.recordOffset("field", "a");
        context.recordOffset("field", "a");
        context.recordOffset("field", "b");
        context.recordOffset("field", "z");
        context.recordOffset("field", "a");
        context.recordOffset("field", "b");

        var parserContext = new TestDocumentParserContext();
        context.addToLuceneDocument(parserContext);

        var binaryDocValues = parserContext.doc().getField("field");
        int[] offsetToOrd = parseOffsetArray(new ByteArrayStreamInput(binaryDocValues.binaryValue().bytes));
        assertArrayEquals(new int[] { 0, 0, 1, 2, 0, 1 }, offsetToOrd);
    }

    public void testOffsetsWithNull() throws IOException {
        var context = new FieldArrayContext();
        context.recordNull("field");
        context.recordOffset("field", "a");
        context.recordOffset("field", "b");
        context.recordOffset("field", "z");
        context.recordNull("field");
        context.recordOffset("field", "b");

        var parserContext = new TestDocumentParserContext();
        context.addToLuceneDocument(parserContext);

        var binaryDocValues = parserContext.doc().getField("field");
        int[] offsetToOrd = parseOffsetArray(new ByteArrayStreamInput(binaryDocValues.binaryValue().bytes));
        assertArrayEquals(new int[] { -1, 0, 1, 2, -1, 1 }, offsetToOrd);
    }

    public void testEmptyOffset() throws IOException {
        var context = new FieldArrayContext();
        context.maybeRecordEmptyArray("field");

        var parserContext = new TestDocumentParserContext();
        context.addToLuceneDocument(parserContext);

        var binaryDocValues = parserContext.doc().getField("field");
        int[] offsetToOrd = parseOffsetArray(new ByteArrayStreamInput(binaryDocValues.binaryValue().bytes));
        assertArrayEquals(new int[] {}, offsetToOrd);
    }

}
