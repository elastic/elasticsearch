/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
import org.elasticsearch.index.mapper.blockloader.docvalues.LongsBlockLoader;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 * Reads keys computed by an evaluator that passes its input through, so a document has a key when it has a single value.
 * Covers dense and sparse fields, documents that have several values, and the bulk read of the ES819 codec.
 */
public class EsqlSortKeyValuesTests extends ESTestCase {

    public void testIteration() throws IOException {
        int numDocs = between(1, 5000);
        boolean multiValued = randomBoolean();
        boolean sparse = randomBoolean();
        Long[] expected = new Long[numDocs];
        IndexWriterConfig config = new IndexWriterConfig();
        if (randomBoolean()) {
            config.setCodec(TestUtil.alwaysDocValuesFormat(new ES819TSDBDocValuesFormat()));
        }
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, config)) {
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                if (sparse && randomInt(3) == 0) {
                    // no value, so no key
                } else if (multiValued && randomInt(3) == 0) {
                    doc.add(new SortedNumericDocValuesField("f", i));
                    doc.add(new SortedNumericDocValuesField("f", i + 1));
                } else {
                    doc.add(new SortedNumericDocValuesField("f", i));
                    expected[i] = (long) i;
                }
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = reader.leaves().getFirst().reader();

                long missing = randomLong();
                PassThrough evaluator = new PassThrough();
                EsqlSortKeyValues values = values(leaf, evaluator, missing);
                long[] keys = new long[numDocs];
                int doc = 0;
                int count;
                while ((count = values.next(keys, doc)) > 0) {
                    assertThat(count, equalTo(Math.min(DocValuesExpressionReader.CHUNK, numDocs - doc)));
                    doc += count;
                    // Reading the last chunk closes the evaluator, since Lucene doesn't move past the last document
                    assertThat(evaluator.closed, equalTo(doc == numDocs ? 1 : 0));
                }
                assertThat(doc, equalTo(numDocs));
                assertThat(values.next(keys, 0), equalTo(0));
                assertThat(evaluator.closed, equalTo(1));
                for (int i = 0; i < numDocs; i++) {
                    assertThat(keys[i], equalTo(expected[i] == null ? missing : expected[i]));
                }
            }
        }
    }

    private static EsqlSortKeyValues values(LeafReader leaf, ExpressionEvaluator evaluator, long missing) throws IOException {
        var reader = new DocValuesExpressionReader(
            leaf.getContext(),
            leaf.maxDoc(),
            List.of(new LongsBlockLoader("f")),
            evaluator,
            EsqlSortKey.LOADER_FACTORY
        );
        return new EsqlSortKeyValues(reader, DataType.LONG, missing);
    }

    private static class PassThrough implements ExpressionEvaluator {
        int closed;

        @Override
        public Block eval(Page page) {
            Block block = page.getBlock(0);
            block.incRef();
            return block;
        }

        @Override
        public long baseRamBytesUsed() {
            return 0;
        }

        @Override
        public void close() {
            closed++;
        }
    }
}
