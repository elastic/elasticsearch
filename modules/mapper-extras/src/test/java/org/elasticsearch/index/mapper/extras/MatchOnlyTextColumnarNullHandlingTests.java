/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.extras;

import org.elasticsearch.columnar.string.DictionaryPolicy;
import org.elasticsearch.columnar.string.StringColumnOptions;
import org.elasticsearch.index.mapper.AbstractColumnarNullHandlingTestCase;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * How a match_only_text field records a {@code null} under the ColumNAR codec.
 */
public class MatchOnlyTextColumnarNullHandlingTests extends AbstractColumnarNullHandlingTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new MapperExtrasPlugin());
    }

    @Override
    protected String fieldTypeName() {
        return "match_only_text";
    }

    public void testTheColumnRecordsNoTermBounds() throws IOException {
        final StringColumnOptions options = ((FieldMapper) codecMapperService().mappingLookup().getMapper(FIELD)).columnarStringOptions();
        assertNotNull("a match_only_text field is written as a string column", options);
        assertEquals(DictionaryPolicy.NONE, options.dictionary());
        assertFalse("no range query reaches a match_only_text column", options.writeSkipIndex());
    }
}
