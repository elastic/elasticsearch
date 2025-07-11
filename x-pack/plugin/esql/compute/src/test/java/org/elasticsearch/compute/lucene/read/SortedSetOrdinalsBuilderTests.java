/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.indices.CrankyCircuitBreakerService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class SortedSetOrdinalsBuilderTests extends ComputeTestCase {

    public void testReader() throws IOException {
        testRead(blockFactory());
    }

    public void testReadWithCranky() throws IOException {
        BlockFactory factory = crankyBlockFactory();
        try {
            testRead(factory);
            // If we made it this far cranky didn't fail us!
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
        assertThat(factory.breaker().getUsed(), equalTo(0L));
    }

    private void testRead(BlockFactory factory) throws IOException {
        int numDocs = between(1, 1000);
        try (Directory directory = newDirectory(); RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
            Map<Integer, List<String>> expectedValues = new HashMap<>();
            List<String> allKeys = IntStream.range(0, between(1, 20)).mapToObj(n -> String.format(Locale.ROOT, "v%02d", n)).toList();
            for (int i = 0; i < numDocs; i++) {
                List<String> subs = randomSubsetOf(allKeys).stream().sorted().toList();
                expectedValues.put(i, subs);
                Document doc = new Document();
                for (String v : subs) {
                    doc.add(new SortedSetDocValuesField("f", new BytesRef(v)));
                }
                doc.add(new NumericDocValuesField("k", i));
                indexWriter.addDocument(doc);
            }
            Map<Integer, List<String>> actualValues = new HashMap<>();
            try (IndexReader reader = indexWriter.getReader()) {
                for (LeafReaderContext ctx : reader.leaves()) {
                    var keysDV = ctx.reader().getNumericDocValues("k");
                    var valuesDV = ctx.reader().getSortedSetDocValues("f");
                    try (
                        var valuesBuilder = new SortedSetOrdinalsBuilder(factory, valuesDV, ctx.reader().numDocs());
                        var keysBuilder = factory.newIntVectorBuilder(ctx.reader().numDocs())
                    ) {
                        for (int i = 0; i < ctx.reader().maxDoc(); i++) {
                            if (ctx.reader().getLiveDocs() == null || ctx.reader().getLiveDocs().get(i)) {
                                assertTrue(keysDV.advanceExact(i));
                                keysBuilder.appendInt(Math.toIntExact(keysDV.longValue()));
                                if (valuesDV.advanceExact(i)) {
                                    int valueCount = valuesDV.docValueCount();
                                    if (valueCount > 1) {
                                        valuesBuilder.beginPositionEntry();
                                    }
                                    for (int v = 0; v < valueCount; v++) {
                                        valuesBuilder.appendOrd(Math.toIntExact(valuesDV.nextOrd()));
                                    }
                                    if (valueCount > 1) {
                                        valuesBuilder.endPositionEntry();
                                    }
                                } else {
                                    valuesBuilder.appendNull();
                                }
                            }
                        }
                        BytesRef scratch = new BytesRef();
                        try (BytesRefBlock valuesBlock = valuesBuilder.build(); IntVector counterVector = keysBuilder.build()) {
                            for (int p = 0; p < valuesBlock.getPositionCount(); p++) {
                                int key = counterVector.getInt(p);
                                ArrayList<String> subs = new ArrayList<>();
                                assertNull(actualValues.put(key, subs));
                                int count = valuesBlock.getValueCount(p);
                                int first = valuesBlock.getFirstValueIndex(p);
                                int last = first + count;
                                for (int i = first; i < last; i++) {
                                    String val = valuesBlock.getBytesRef(i, scratch).utf8ToString();
                                    subs.add(val);
                                }
                            }
                        }
                    }
                }
            }
            assertThat(actualValues, equalTo(expectedValues));
        }
    }

    public void testAllNull() throws IOException {
        BlockFactory factory = blockFactory();
        int numDocs = between(1, 100);
        try (Directory directory = newDirectory(); RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                doc.add(new SortedSetDocValuesField("f", new BytesRef("empty")));
                indexWriter.addDocument(doc);
            }
            try (IndexReader reader = indexWriter.getReader()) {
                for (LeafReaderContext ctx : reader.leaves()) {
                    var docValues = ctx.reader().getSortedSetDocValues("f");
                    try (BlockLoader.OrdinalsBuilder builder = new SortedSetOrdinalsBuilder(factory, docValues, ctx.reader().numDocs())) {
                        for (int i = 0; i < ctx.reader().maxDoc(); i++) {
                            if (ctx.reader().getLiveDocs() == null || ctx.reader().getLiveDocs().get(i)) {
                                assertThat(docValues.advanceExact(i), equalTo(true));
                                builder.appendNull();
                            }
                        }
                        try (BytesRefBlock built = (BytesRefBlock) builder.build()) {
                            for (int p = 0; p < built.getPositionCount(); p++) {
                                assertThat(built.isNull(p), equalTo(true));
                            }
                            assertThat(built.areAllValuesNull(), equalTo(true));
                        }
                    }
                }
            }
        }
    }
}
