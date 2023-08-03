/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.elasticsearch.index.fielddata.SourceValueFetcherSortedBooleanIndexFieldData.SourceValueFetcherSortedBooleanDocValues;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;
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

        SourceValueFetcherSortedBooleanDocValues values = new SourceValueFetcherSortedBooleanDocValues(null, new ValueFetcher() {
            @Override
            public List<Object> fetchValues(Source source, int doc, List<Object> ignoredValues) throws IOException {
                return docs.get(doc);
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                return StoredFieldsSpec.NO_REQUIREMENTS;
            }
        }, (ctx, doc) -> null);

        for (int doc = 0; doc < docs.size(); ++doc) {
            values.advanceExact(doc);

            int docTrues = 0;
            int docFalses = 0;

            for (Object object : docs.get(doc)) {
                if ((boolean) object) {
                    ++docTrues;
                } else {
                    ++docFalses;
                }
            }

            assertEquals(docs.get(doc).size(), values.docValueCount());

            int valueTrues = 0;
            int valueFalses = 0;

            for (int count = 0; count < values.docValueCount(); ++count) {
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
