/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class TextFieldColumnarQueryTests extends AbstractColumnarBinaryLayoutTestCase {

    @Override
    protected String fieldTypeName() {
        return "text";
    }

    @Override
    protected BinaryDocValuesFormat binaryFormatOf(MappedFieldType fieldType) {
        return ((TextFieldMapper.TextFieldType) fieldType).binaryFormat();
    }

    /** A range over a text field is answered from terms, so without them it is refused rather than answered wrongly. */
    public void testARangeIsRefusedRatherThanAnsweredFromTheColumn() throws IOException {
        forEachLayout((field, context, hits) -> {
            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> ((StringFieldType) field).rangeQuery("alpha", "delta", true, false, context)
            );
            assertThat(e.getMessage(), containsString("Cannot search on field [" + FIELD + "] since it is not indexed"));
        });
    }
}
