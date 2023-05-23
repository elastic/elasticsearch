/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.script.AbstractFieldScript;
import org.elasticsearch.script.SortedNumericDocValuesLongFieldScript;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SortedNumericDocValuesLongFieldScriptTests extends ESTestCase {

    public void testValuesLimitIsNotEnforced() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            FieldType fieldType = new FieldType();
            fieldType.setDocValuesType(DocValuesType.BINARY);
            List<IndexableField> fields = new ArrayList<>();
            int numValues = AbstractFieldScript.MAX_VALUES + randomIntBetween(1, 100);
            for (int i = 0; i < numValues; i++) {
                fields.add(new SortedNumericDocValuesField("test", i));
            }
            iw.addDocument(fields);
            try (DirectoryReader reader = iw.getReader()) {
                SortedNumericDocValuesLongFieldScript docValues = new SortedNumericDocValuesLongFieldScript(
                    "test",
                    new SearchLookup(field -> null, (ft, lookup, ftd) -> null, (ctx, doc) -> null),
                    reader.leaves().get(0)
                );
                List<Long> values = new ArrayList<>();
                docValues.runForDoc(0, values::add);
                assertEquals(numValues, values.size());
            }
        }
    }
}
