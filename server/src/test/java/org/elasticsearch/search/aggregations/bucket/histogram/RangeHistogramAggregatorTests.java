package org.elasticsearch.search.aggregations.bucket.histogram;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.RangeFieldMapper;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.support.ValueType;

import java.util.Collections;

public class RangeHistogramAggregatorTests extends AggregatorTestCase {
        public void testDoubleRanges() throws Exception {

        RangeFieldMapper.RangeType rangeType = RangeFieldMapper.RangeType.DOUBLE;
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            BytesRef encodedRange =
                rangeType.encodeRanges(Collections.singleton(new RangeFieldMapper.Range(rangeType, 1.0D, 3.0D, true, true)));
            doc.add(new BinaryDocValuesField("field", encodedRange));
            w.addDocument(doc);

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg", ValueType.RANGE)
                .field("field")
                .interval(5);
            MappedFieldType fieldType = new RangeFieldMapper.Builder("field", rangeType).fieldType();
            fieldType.setName("field");

            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = search(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
                assertEquals(1, histogram.getBuckets().size());
            }

        }
    }

}
