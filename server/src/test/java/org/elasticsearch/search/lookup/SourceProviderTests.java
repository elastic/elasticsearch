/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.lookup;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class SourceProviderTests extends ESTestCase {

    public void testStoredFieldsSourceProvider() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            doc.add(new StringField("field", "value", Field.Store.YES));
            doc.add(new StoredField("_source", new BytesRef("{\"field\": \"value\"}")));
            iw.addDocument(doc);

            try (IndexReader reader = iw.getReader()) {
                LeafReaderContext readerContext = reader.leaves().get(0);

                SourceProvider sourceProvider = SourceProvider.fromStoredFields();
                Source source = sourceProvider.getSource(readerContext, 0);

                assertNotNull(source.internalSourceRef());

                // Source should be preserved if we pass in the same reader and document
                Source s2 = sourceProvider.getSource(readerContext, 0);
                assertSame(s2, source);
            }
        }
    }
}
