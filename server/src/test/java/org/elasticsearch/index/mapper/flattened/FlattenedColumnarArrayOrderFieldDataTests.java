/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Fielddata coverage for flattened fields in strictly columnar mode, where values are stored via
 * {@link org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.KeyedArrayOrderInlineNull} and read back through
 * {@link org.elasticsearch.index.fielddata.KeyFilteredSortingArrayOrderBinaryDocValues}. Mirrors the scenarios in
 * {@link org.elasticsearch.index.mapper.AbstractColumnarArrayOrderFieldDataTestCase}, adapted for flattened's keyed sub-field access
 * pattern ({@code field.key} rather than {@code field}).
 */
public class FlattenedColumnarArrayOrderFieldDataTests extends MapperServiceTestCase {

    private MapperService columnarMapperService() throws IOException {
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
        return createMapperService(
            settings,
            mapping(b -> b.startObject("field").field("type", "flattened").field("preserve_leaf_arrays", "exact").endObject())
        );
    }

    /**
     * Indexes a single document and returns the fielddata values for {@code field.<key>}, or {@code null} when the key has no non-null
     * values for that document.
     */
    private List<String> fielddataValues(String key, CheckedConsumer<XContentBuilder, IOException> doc) throws IOException {
        MapperService mapperService = columnarMapperService();
        assertTrue(
            "flattened columnar path must store array values in order",
            mapperService.documentMapper().mappers().getMapper("field").storesArrayValuesInOrder()
        );
        List<String> values = new ArrayList<>();
        boolean[] hasValues = { false };
        withLuceneIndex(mapperService, iw -> iw.addDocument(mapperService.documentMapper().parse(source(doc)).rootDoc()), reader -> {
            LeafReaderContext leaf = reader.leaves().get(0);
            IndexFieldData<?> ifd = mapperService.fieldType("field." + key)
                .fielddataBuilder(FieldDataContext.noRuntimeFields("test", "test"))
                .build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService());
            SortedBinaryDocValues dv = ifd.load(leaf).getBytesValues();
            if (dv.advanceExact(0)) {
                hasValues[0] = true;
                int count = dv.docValueCount();
                for (int i = 0; i < count; i++) {
                    values.add(dv.nextValue().utf8ToString());
                }
            }
        });
        return hasValues[0] ? values : null;
    }

    public void testMultiValuedReadsBackSorted() throws IOException {
        assertEquals(
            List.of("apple", "banana", "cherry"),
            fielddataValues(
                "k",
                b -> b.startObject("field").startArray("k").value("banana").value("apple").value("cherry").endArray().endObject()
            )
        );
    }

    public void testDuplicatesDeduplicatedInFielddata() throws IOException {
        assertEquals(
            List.of("apple", "banana"),
            fielddataValues(
                "k",
                b -> b.startObject("field").startArray("k").value("apple").value("apple").value("banana").endArray().endObject()
            )
        );
    }

    public void testNullsDropped() throws IOException {
        assertEquals(
            List.of("apple", "cherry"),
            fielddataValues(
                "k",
                b -> b.startObject("field").startArray("k").value("apple").nullValue().value("cherry").endArray().endObject()
            )
        );
    }

    public void testSingleValue() throws IOException {
        assertEquals(List.of("apple"), fielddataValues("k", b -> b.startObject("field").field("k", "apple").endObject()));
    }

    public void testAllNullArrayHasNoValues() throws IOException {
        assertNull(fielddataValues("k", b -> b.startObject("field").startArray("k").nullValue().nullValue().endArray().endObject()));
    }

    public void testKeyIsolation() throws IOException {
        // Values for key "a" must not appear under key "z" and vice versa.
        CheckedConsumer<XContentBuilder, IOException> doc = b -> b.startObject("field")
            .startArray("a")
            .value("banana")
            .value("apple")
            .endArray()
            .field("z", "other")
            .endObject();
        assertEquals(List.of("apple", "banana"), fielddataValues("a", doc));
        assertEquals(List.of("other"), fielddataValues("z", doc));
    }

    /**
     * Exercises the {@link org.elasticsearch.lucene.queries.ScanningBinaryDocValuesTermQuery} with
     * {@code keyedInlineNull=true} end-to-end: indexes multiple documents and verifies that
     * {@link org.elasticsearch.index.mapper.blockloader.docvalues.MultiValueSeparateCountBinaryDocValuesReader#matchKeyedInlineNull}
     * correctly skips null slots and matches only the target value.
     */
    public void testTermQueryMatchesCorrectDocuments() throws IOException {
        MapperService mapperService = columnarMapperService();
        // doc0: two values for "k"; doc1: scalar; doc2: array with an inline null slot.
        withLuceneIndex(mapperService, iw -> {
            iw.addDocument(
                mapperService.documentMapper()
                    .parse(source(b -> b.startObject("field").startArray("k").value("apple").value("banana").endArray().endObject()))
                    .rootDoc()
            );
            iw.addDocument(
                mapperService.documentMapper().parse(source(b -> b.startObject("field").field("k", "cherry").endObject())).rootDoc()
            );
            iw.addDocument(
                mapperService.documentMapper()
                    .parse(
                        source(b -> b.startObject("field").startArray("k").value("banana").nullValue().value("date").endArray().endObject())
                    )
                    .rootDoc()
            );
        }, reader -> {
            IndexSearcher searcher = new IndexSearcher(reader);
            Query appleQ = mapperService.fieldType("field.k").termQuery("apple", null);
            Query bananaQ = mapperService.fieldType("field.k").termQuery("banana", null);
            Query cherryQ = mapperService.fieldType("field.k").termQuery("cherry", null);
            Query dateQ = mapperService.fieldType("field.k").termQuery("date", null);
            Query missingQ = mapperService.fieldType("field.k").termQuery("missing", null);
            assertEquals(1, searcher.count(appleQ));   // only doc0
            assertEquals(2, searcher.count(bananaQ));  // doc0 and doc2
            assertEquals(1, searcher.count(cherryQ));  // only doc1
            assertEquals(1, searcher.count(dateQ));    // only doc2 (after null slot)
            assertEquals(0, searcher.count(missingQ)); // no match
        });
    }
}
