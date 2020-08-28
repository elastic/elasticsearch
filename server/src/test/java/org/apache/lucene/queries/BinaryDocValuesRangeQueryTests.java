/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lucene.queries;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.RangeFieldMapper;
import org.elasticsearch.index.mapper.RangeType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static java.util.Collections.singleton;
import static org.apache.lucene.queries.BinaryDocValuesRangeQuery.QueryType.CONTAINS;
import static org.apache.lucene.queries.BinaryDocValuesRangeQuery.QueryType.CROSSES;
import static org.apache.lucene.queries.BinaryDocValuesRangeQuery.QueryType.INTERSECTS;
import static org.apache.lucene.queries.BinaryDocValuesRangeQuery.QueryType.WITHIN;

public class BinaryDocValuesRangeQueryTests extends ESTestCase {

    public void testBasics() throws Exception {
        String fieldName = "long_field";
        RangeType rangeType = RangeType.LONG;
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                // intersects (within)
                Document document = new Document();
                BytesRef encodedRange =
                        rangeType.encodeRanges(singleton(new RangeFieldMapper.Range(rangeType, -10L, 9L, true , true)));
                document.add(new BinaryDocValuesField(fieldName, encodedRange));
                writer.addDocument(document);

                // intersects (crosses)
                document = new Document();
                encodedRange = rangeType.encodeRanges(singleton(new RangeFieldMapper.Range(rangeType, 10L, 20L, true , true)));
                document.add(new BinaryDocValuesField(fieldName, encodedRange));
                writer.addDocument(document);

                // intersects (contains, crosses)
                document = new Document();
                encodedRange = rangeType.encodeRanges(singleton(new RangeFieldMapper.Range(rangeType, -20L, 30L, true , true)));
                document.add(new BinaryDocValuesField(fieldName, encodedRange));
                writer.addDocument(document);

                // intersects (within)
                document = new Document();
                encodedRange = rangeType.encodeRanges(singleton(new RangeFieldMapper.Range(rangeType, -11L, 1L, true , true)));
                document.add(new BinaryDocValuesField(fieldName, encodedRange));
                writer.addDocument(document);

                // intersects (crosses)
                document = new Document();
                encodedRange = rangeType.encodeRanges(singleton(new RangeFieldMapper.Range(rangeType, 12L, 15L, true , true)));
                document.add(new BinaryDocValuesField(fieldName, encodedRange));
                writer.addDocument(document);

                // disjoint
                document = new Document();
                encodedRange = rangeType.encodeRanges(singleton(new RangeFieldMapper.Range(rangeType, -122L, -115L, true , true)));
                document.add(new BinaryDocValuesField(fieldName, encodedRange));
                writer.addDocument(document);

                // intersects (crosses)
                document = new Document();
                encodedRange = rangeType.encodeRanges(singleton(new RangeFieldMapper.Range(rangeType, Long.MIN_VALUE, -11L, true , true)));
                document.add(new BinaryDocValuesField(fieldName, encodedRange));
                writer.addDocument(document);

                // equal (within, contains, intersects)
                document = new Document();
                encodedRange = rangeType.encodeRanges(singleton(new RangeFieldMapper.Range(rangeType, -11L, 15L, true , true)));
                document.add(new BinaryDocValuesField(fieldName, encodedRange));
                writer.addDocument(document);

                // intersects, within
                document = new Document();
                encodedRange = rangeType.encodeRanges(singleton(new RangeFieldMapper.Range(rangeType, 5L, 10L, true , true)));
                document.add(new BinaryDocValuesField(fieldName, encodedRange));
                writer.addDocument(document);

                // search
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query query = rangeType.dvRangeQuery(fieldName, INTERSECTS, -11L, 15L, true, true);
                    assertEquals(8, searcher.count(query));
                    query = rangeType.dvRangeQuery(fieldName, WITHIN, -11L, 15L, true, true);
                    assertEquals(5, searcher.count(query));
                    query = rangeType.dvRangeQuery(fieldName, CONTAINS, -11L, 15L, true, true);
                    assertEquals(2, searcher.count(query));
                    query = rangeType.dvRangeQuery(fieldName, CROSSES, -11L, 15L, true, true);
                    assertEquals(3, searcher.count(query));

                    // test includeFrom = false and includeTo = false
                    query = rangeType.dvRangeQuery(fieldName, INTERSECTS, -11L, 15L, false, false);
                    assertEquals(7, searcher.count(query));
                    query = rangeType.dvRangeQuery(fieldName, WITHIN, -11L, 15L, false, false);
                    assertEquals(2, searcher.count(query));
                    query = rangeType.dvRangeQuery(fieldName, CONTAINS, -11L, 15L, false, false);
                    assertEquals(2, searcher.count(query));
                    query = rangeType.dvRangeQuery(fieldName, CROSSES, -11L, 15L, false, false);
                    assertEquals(5, searcher.count(query));
                }
            }
        }
    }

    public void testNoField() throws IOException {
        String fieldName = "long_field";
        RangeType rangeType = RangeType.LONG;

        // no field in index
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                writer.addDocument(new Document());
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query query = rangeType.dvRangeQuery(fieldName, INTERSECTS, -1L, 1L, true, true);
                    assertEquals(0, searcher.count(query));
                }
            }
        }

        // no field in segment
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                // intersects (within)
                Document document = new Document();
                BytesRef encodedRange =
                    rangeType.encodeRanges(singleton(new RangeFieldMapper.Range(rangeType, 0L, 0L, true , true)));
                document.add(new BinaryDocValuesField(fieldName, encodedRange));
                writer.addDocument(document);
                writer.commit();
                writer.addDocument(new Document());
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query query = rangeType.dvRangeQuery(fieldName, INTERSECTS, -1L, 1L, true, true);
                    assertEquals(1, searcher.count(query));
                }
            }
        }
    }

}
