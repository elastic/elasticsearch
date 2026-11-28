/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.filter;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.test.ESTestCase;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.function.IntPredicate;

/**
 * Three segments of random docs with a category, a nullable price and a quantity, some of them deleted. The arrays are
 * the brute-force answer a filter over the index has to match. {@code price} and {@code quantity} are doc values, so an
 * {@link ExpressionDocFilter} can read them through {@link Columns}.
 */
final class DocFilterFixture implements Closeable {
    static final int SEGMENTS = 3;
    static final int DOCS_PER_SEGMENT = 500;
    static final long THRESHOLD = 100;

    final String[] category;
    final Long[] price;
    final Long[] quantity;
    final boolean[] deleted;
    final Directory directory;
    final DirectoryReader reader;
    final IndexSearcher searcher;

    DocFilterFixture() throws IOException {
        int docs = SEGMENTS * DOCS_PER_SEGMENT;
        category = new String[docs];
        price = new Long[docs];
        quantity = new Long[docs];
        deleted = new boolean[docs];
        directory = ESTestCase.newDirectory();
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
            for (int i = 0; i < docs; i++) {
                category[i] = ESTestCase.randomBoolean() ? "a" : "b";
                price[i] = ESTestCase.randomInt(9) == 0 ? null : (long) ESTestCase.randomIntBetween(1, 40);
                quantity[i] = (long) ESTestCase.randomIntBetween(1, 40);
                Document document = new Document();
                document.add(new StringField("id", Integer.toString(i), Field.Store.NO));
                document.add(new KeywordField("category", category[i], Field.Store.NO));
                if (price[i] != null) {
                    document.add(new LongField("price", price[i], Field.Store.NO));
                }
                document.add(new LongField("quantity", quantity[i], Field.Store.NO));
                writer.addDocument(document);
                if ((i + 1) % DOCS_PER_SEGMENT == 0) {
                    writer.commit();
                }
            }
            for (int i = 0; i < docs; i++) {
                if (ESTestCase.randomInt(19) == 0) {
                    deleted[i] = true;
                    writer.deleteDocuments(new Term("id", Integer.toString(i)));
                }
            }
            writer.commit();
        }
        reader = DirectoryReader.open(directory);
        searcher = new IndexSearcher(reader);
        searcher.setQueryCache(null);
    }

    NativeDocFilter category(String value) throws IOException {
        return NativeDocFilter.of(searcher, new TermQuery(new Term("category", value)));
    }

    boolean expensive(int doc) {
        return price[doc] != null && price[doc] * quantity[doc] > THRESHOLD;
    }

    /** The global ids of the live docs {@code keep} accepts. */
    TreeSet<Integer> expected(IntPredicate keep) {
        TreeSet<Integer> docs = new TreeSet<>();
        for (int doc = 0; doc < category.length; doc++) {
            if (deleted[doc] == false && keep.test(doc)) {
                docs.add(doc);
            }
        }
        return docs;
    }

    @Override
    public void close() throws IOException {
        reader.close();
        directory.close();
    }

    /** Loads a long column from doc values and records the docs it was asked for. */
    static final class Columns implements DocColumns {
        private final BlockFactory factory;
        final List<Integer> loaded = new ArrayList<>();

        Columns(BlockFactory factory) {
            this.factory = factory;
        }

        @Override
        public Block load(String field, LeafReaderContext leaf, int[] docs) throws IOException {
            SortedNumericDocValues values = leaf.reader().getSortedNumericDocValues(field);
            try (LongBlock.Builder builder = factory.newLongBlockBuilder(docs.length)) {
                for (int doc : docs) {
                    if (field.equals("price")) {
                        loaded.add(leaf.docBase + doc);
                    }
                    if (values != null && values.advanceExact(doc) && values.docValueCount() == 1) {
                        builder.appendLong(values.nextValue());
                    } else {
                        builder.appendNull();
                    }
                }
                return builder.build();
            }
        }
    }

    /** {@code price * quantity > threshold}, null when either factor is null. */
    static final class ProductGreaterThan implements ExpressionEvaluator {
        private final BlockFactory factory;
        private final long threshold;

        ProductGreaterThan(BlockFactory factory, long threshold) {
            this.factory = factory;
            this.threshold = threshold;
        }

        @Override
        public Block eval(Page page) {
            LongBlock left = page.getBlock(0);
            LongBlock right = page.getBlock(1);
            try (BooleanBlock.Builder builder = factory.newBooleanBlockBuilder(page.getPositionCount())) {
                for (int position = 0; position < page.getPositionCount(); position++) {
                    if (left.isNull(position) || right.isNull(position)) {
                        builder.appendNull();
                    } else {
                        long product = left.getLong(left.getFirstValueIndex(position)) * right.getLong(right.getFirstValueIndex(position));
                        builder.appendBoolean(product > threshold);
                    }
                }
                return builder.build();
            }
        }

        @Override
        public long baseRamBytesUsed() {
            return 0;
        }

        @Override
        public void close() {}
    }

    ExpressionDocFilter expensive(Columns columns, BlockFactory factory) {
        return new ExpressionDocFilter(new ProductGreaterThan(factory, THRESHOLD), List.of("price", "quantity"), columns);
    }
}
