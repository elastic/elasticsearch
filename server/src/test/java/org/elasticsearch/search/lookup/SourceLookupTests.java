/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.lookup;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class SourceLookupTests extends ESTestCase {

    public void testSetSegmentAndDocument() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            doc.add(new StringField("field", "value", Field.Store.YES));
            iw.addDocument(doc);

            try (IndexReader reader = iw.getReader()) {
                LeafReaderContext readerContext = reader.leaves().get(0);

                SourceLookup sourceLookup = new SourceLookup();
                sourceLookup.setSegmentAndDocument(readerContext, 42);
                sourceLookup.setSource(
                    BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", "value").endObject())
                );
                assertNotNull(sourceLookup.internalSourceRef());

                // Source should be preserved if we pass in the same reader and document
                sourceLookup.setSegmentAndDocument(readerContext, 42);
                assertNotNull(sourceLookup.internalSourceRef());

                // Check that the stored fields reader is not loaded eagerly
                LeafReader throwingReader = new SequentialStoredFieldsLeafReader(readerContext.reader()) {
                    @Override
                    protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
                        throw new UnsupportedOperationException("attempted to load stored fields reader");
                    }

                    @Override
                    public CacheHelper getReaderCacheHelper() {
                        return in.getReaderCacheHelper();
                    }

                    @Override
                    public CacheHelper getCoreCacheHelper() {
                        return in.getCoreCacheHelper();
                    }
                };

                sourceLookup.setSegmentAndDocument(throwingReader.getContext(), 0);
                ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, sourceLookup::source);
                assertThat(e.getCause(), instanceOf(UnsupportedOperationException.class));
                assertThat(e.getCause().getMessage(), containsString("attempted to load stored fields reader"));
            }
        }
    }
}
