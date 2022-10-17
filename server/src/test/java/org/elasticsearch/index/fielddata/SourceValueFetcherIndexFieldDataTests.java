/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.SourceValueFetcherSortedBooleanIndexFieldData.SourceValueFetcherSortedBooleanDocValues;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

public class SourceValueFetcherIndexFieldDataTests extends ESTestCase {

    public void testSourceValueFetcherSortedBooleanDocValues() throws IOException {
        List<List<Object>> docs = List.of(
            List.of(randomBoolean()),
            List.of(randomBoolean(), randomBoolean()),
            List.of(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()),
            List.of(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean())
        );

        int[] doc = new int[1];

        SourceValueFetcherSortedBooleanDocValues values = new SourceValueFetcherSortedBooleanDocValues(
            null,
            (source, ignoredValues) -> docs.get(doc[0]++),
            new SourceLookup(null) {
                @Override
                public void setSegmentAndDocument(LeafReaderContext context, int docId) {
                    // do nothing
                }
            }
        );

        for (int i = 0; i < docs.size(); ++i) {
            values.advanceExact(i);

            int docTrues = 0;
            int docFalses = 0;

            for (Object object : docs.get(i)) {
                if ((boolean) object) {
                    ++docTrues;
                } else {
                    ++docFalses;
                }
            }

            assertEquals(docs.get(i).size(), values.docValueCount());

            int valueTrues = 0;
            int valueFalses = 0;

            for (int j = 0; j < values.docValueCount(); ++j) {
                long value = values.nextValue();

                if (value == 1L) {
                    ++valueTrues;
                } else if (value == 0L) {
                    ++valueFalses;
                } else {
                    throw new IllegalStateException("expected 0L or 1L for boolean value, found [" + value + "]");
                }
            }

            assertEquals(docTrues, valueTrues);
            assertEquals(docFalses, valueFalses);
        }
    }
}
