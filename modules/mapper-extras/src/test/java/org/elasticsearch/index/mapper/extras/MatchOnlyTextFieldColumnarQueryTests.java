/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.extras;

import org.elasticsearch.common.CheckedIntFunction;
import org.elasticsearch.index.mapper.AbstractColumnarBinaryLayoutTestCase;
import org.elasticsearch.index.mapper.BinaryDocValuesFormat;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.StringFieldType;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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

    /**
     * The values a phrase is confirmed against, taken straight from the fetcher that reads them. A decoder handed
     * the wrong layout returns other bytes, and here that is the document holding something it was not given -
     * which a phrase query cannot always show, since the analyser drops the framing bytes a mis-read blob carries
     * and can leave the same terms behind.
     */
    public void testTheFetcherReadsTheFieldsValues() throws IOException {
        // The fetcher reads the doc values whether or not the field is indexed, so this reads them without an index.
        forEachLayoutIndex(false, (layout, mapperService, reader, documents) -> {
            final MatchOnlyTextFieldMapper.MatchOnlyTextFieldType field = (MatchOnlyTextFieldMapper.MatchOnlyTextFieldType) mapperService
                .fieldType(FIELD);
            final CheckedIntFunction<List<Object>, IOException> fetcher = field.getValueFetcherProvider(
                createSearchExecutionContext(mapperService)
            ).apply(reader.leaves().get(0));
            for (int doc = 0; doc < documents.size(); doc++) {
                final List<String> actual = new ArrayList<>();
                for (Object value : fetcher.apply(doc)) {
                    actual.add(value.toString());
                }
                Collections.sort(actual);
                assertEquals(layout + " doc " + doc, storedValues(documents, doc), actual);
            }
        });
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
