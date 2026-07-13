/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.search.lookup.FieldLookup;
import org.elasticsearch.search.lookup.LeafFieldLookupProvider;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.List;

public class PopulateFieldLookupTests extends MapperServiceTestCase {
    public void testPopulateFieldLookup() throws IOException {
        final XContentBuilder mapping = createMapping();
        final MapperService mapperService = createMapperService(mapping);
        withLuceneIndex(mapperService, iw -> {
            final Document doc = new Document();
            doc.add(new StoredField("integer", 101));
            doc.add(new StoredField("keyword", new BytesRef("foobar")));
            iw.addDocument(doc);
        }, reader -> {
            final StoredFields storedFields = reader.storedFields();
            final Document document = storedFields.document(0);
            final List<String> documentFields = document.getFields().stream().map(IndexableField::name).toList();
            assertThat(documentFields, Matchers.containsInAnyOrder("integer", "keyword"));

            final IndexSearcher searcher = newSearcher(reader);
            final LeafReaderContext readerContext = searcher.getIndexReader().leaves().get(0);
            final LeafFieldLookupProvider provider = LeafFieldLookupProvider.fromStoredFields().apply(readerContext);
            final FieldLookup integerFieldLookup = new FieldLookup(mapperService.fieldType("integer"));
            final FieldLookup keywordFieldLookup = new FieldLookup(mapperService.fieldType("keyword"));
            provider.populateFieldLookup(integerFieldLookup, 0);
            provider.populateFieldLookup(keywordFieldLookup, 0);
            assertEquals(List.of(101), integerFieldLookup.getValues());
            assertEquals(List.of("foobar"), keywordFieldLookup.getValues());
        });
    }

    private static XContentBuilder createMapping() throws IOException {
        final XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("_doc");
        {
            mapping.startObject("properties");
            {
                mapping.startObject("integer");
                {
                    mapping.field("type", "integer").field("store", "true");
                }
                mapping.endObject();
                mapping.startObject("keyword");
                {
                    mapping.field("type", "keyword").field("store", "true");
                }
                mapping.endObject();
            }
            mapping.endObject();

        }
        return mapping.endObject().endObject();
    }
}
