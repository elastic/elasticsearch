/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.extras;

import org.elasticsearch.index.mapper.AbstractColumnarBinaryLayoutTestCase;
import org.elasticsearch.index.mapper.BinaryDocValuesFormat;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.StringFieldType;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class MatchOnlyTextFieldColumnarQueryTests extends AbstractColumnarBinaryLayoutTestCase {

    @Override
    protected Collection<Plugin> getPlugins() {
        return List.of(new MapperExtrasPlugin());
    }

    @Override
    protected String fieldTypeName() {
        return "match_only_text";
    }

    @Override
    protected BinaryDocValuesFormat binaryFormatOf(MappedFieldType fieldType) {
        return ((MatchOnlyTextFieldMapper.MatchOnlyTextFieldType) fieldType).binaryFormat();
    }

    /** A match_only_text field stores no positions, so a phrase is confirmed against the values it reads back. */
    @Override
    protected boolean confirmsPhrasesFromValues() {
        return true;
    }

    /** A range over a match_only_text field is answered from terms, so without them it is refused. */
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
