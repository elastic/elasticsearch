/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.fn;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.ArrayOrderInlineNull;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.index.mapper.blockloader.MockWarnings;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromBinaryMultiSeparateCountBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromBinaryMultiSeparateCountBlockLoader.ArrayOrderSource;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.ToIntFunction;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.nullValue;

/**
 * Correctness tests for the {@code ArrayOrderInlineNull} pushdown readers (MV_MAX / MV_MIN / BYTE_LENGTH / LENGTH). The
 * {@code .counts} field of this format counts every slot INCLUDING nulls, and the block loader drops nulls, so pushdown must operate on the
 * non-null slots only. Each function's reader is checked against the plain array-order loader (the load-then-compute reference), which
 * already drops nulls, across a matrix that mixes single values, multi-values, interleaved nulls, lone nulls, all-null / empty arrays and
 * missing documents.
 */
public class ArrayOrderInlineNullFnBlockLoaderTests extends ESTestCase {

    private static final String FIELD = "field";
    private static final CircuitBreaker BREAKER = new NoopCircuitBreaker("test");

    private static BytesRef b(String s) {
        return new BytesRef(s);
    }

    /** A doc's slots; a {@code null} element is a null slot, an empty list is an empty array, and a {@code null} document is missing. */
    private static List<BytesRef> doc(BytesRef... slots) {
        List<BytesRef> l = new ArrayList<>();
        Collections.addAll(l, slots);
        return l;
    }

    /**
     * Representative documents plus randomized ones. Always includes at least one document with two or more slots so the {@code .counts}
     * skipper reports {@code maxValue >= 2} and the array-order reader path (not the single-value fast path) is exercised.
     */
    private List<List<BytesRef>> scenarioDocs() {
        List<List<BytesRef>> docs = new ArrayList<>();
        docs.add(doc(b("abc")));                       // single value
        docs.add(doc(b("a"), b("z"), b("m")));         // multi-value, no nulls
        docs.add(doc(null, b("abc")));                 // value + null -> single after drop
        docs.add(doc(b("a"), null, b("b")));           // interleaved -> multi after drop
        docs.add(doc(b("x"), null));                   // value + trailing null -> single after drop
        docs.add(doc((BytesRef) null));                // lone null -> null
        docs.add(doc(null, null));                     // all null -> null
        docs.add(doc());                               // empty array -> null
        docs.add(doc(b("")));                          // single empty string (distinct from null)
        docs.add(null);                                // missing document -> null
        for (int i = 0; i < between(20, 60); i++) {
            docs.add(randomDoc());
        }
        return docs;
    }

    private List<BytesRef> randomDoc() {
        int slotCount = between(1, 8);
        List<BytesRef> slots = new ArrayList<>(slotCount);
        for (int i = 0; i < slotCount; i++) {
            // ASCII values keep byte length == code-point count so BYTE_LENGTH and LENGTH agree on a single expected value.
            slots.add(randomBoolean() ? null : b(randomAlphanumericOfLength(between(0, 10))));
        }
        return slots;
    }

    private void withIndex(List<List<BytesRef>> docs, CheckedConsumer<LeafReaderContext, IOException> body) throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            for (List<BytesRef> slots : docs) {
                if (slots == null) {
                    iw.addDocument(List.of()); // missing document: no field, no count
                    continue;
                }
                ArrayOrderInlineNull field = new ArrayOrderInlineNull(FIELD);
                for (BytesRef slot : slots) {
                    if (slot == null) {
                        field.addNull();
                    } else {
                        field.add(slot);
                    }
                }
                List<IndexableField> fields = new ArrayList<>();
                if (field.hasNonNullValue()) {
                    fields.add(field); // no binary blob is written for an all-null / empty-array document
                }
                fields.add(NumericDocValuesField.indexedField(field.countFieldName(), field.count()));
                iw.addDocument(fields);
            }
            iw.forceMerge(1);
            try (DirectoryReader dr = iw.getReader()) {
                body.accept(getOnlyLeafReader(dr).getContext());
            }
        }
    }

    private static TestBlock read(BlockLoader.ColumnAtATimeReader reader, BlockLoader.Docs docs) throws IOException {
        return (TestBlock) reader.read(TestBlock.factory(), docs, 0, false);
    }

    private static BlockLoader.ColumnAtATimeReader referenceReader(CircuitBreaker breaker, LeafReaderContext ctx) throws IOException {
        return new BytesRefsFromBinaryMultiSeparateCountBlockLoader(FIELD, ArrayOrderSource.INLINE).reader(breaker, ctx);
    }

    public void testMvMax() throws IOException {
        withIndex(scenarioDocs(), ctx -> {
            var loader = new MvMaxBytesRefsFromBinaryBlockLoader(FIELD, ArrayOrderSource.INLINE);
            try (var reference = referenceReader(BREAKER, ctx); var reader = loader.reader(BREAKER, ctx)) {
                assertThat(reader, hasToString("MvMaxBytesRefsFromBinary.ArrayOrderInlineNull"));
                BlockLoader.Docs docs = TestBlock.docs(ctx);
                try (TestBlock ref = read(reference, docs); TestBlock max = read(reader, docs)) {
                    checkExtreme(ref, max, Comparator.naturalOrder());
                }
            }
        });
    }

    public void testMvMin() throws IOException {
        withIndex(scenarioDocs(), ctx -> {
            var loader = new MvMinBytesRefsFromBinaryBlockLoader(FIELD, ArrayOrderSource.INLINE);
            try (var reference = referenceReader(BREAKER, ctx); var reader = loader.reader(BREAKER, ctx)) {
                assertThat(reader, hasToString("MvMinBytesRefsFromBinary.ArrayOrderInlineNull"));
                BlockLoader.Docs docs = TestBlock.docs(ctx);
                try (TestBlock ref = read(reference, docs); TestBlock min = read(reader, docs)) {
                    checkExtreme(ref, min, Comparator.reverseOrder());
                }
            }
        });
    }

    public void testByteLength() throws IOException {
        checkLength("ByteLengthFromBytesRef.MultiValuedBinaryArrayOrderInlineNull", warnings -> {
            var loader = new ByteLengthFromBytesRefDocValuesBlockLoader(warnings, FIELD, ArrayOrderSource.INLINE);
            return loader;
        }, bytes -> bytes.length);
    }

    public void testLength() throws IOException {
        checkLength("Utf8CodePointsFromOrds.MultiValuedBinaryArrayOrderInlineNull", warnings -> {
            var loader = new Utf8CodePointsFromOrdsBlockLoader(
                warnings,
                FIELD,
                ByteSizeValue.ofKb(between(1, 100)),
                ArrayOrderSource.INLINE
            );
            return loader;
        }, bytes -> bytes.utf8ToString().codePointCount(0, bytes.utf8ToString().length()));
    }

    private void checkLength(
        String expectedToString,
        java.util.function.Function<MockWarnings, BlockDocValuesReader.DocValuesBlockLoader> loaderFactory,
        ToIntFunction<BytesRef> length
    ) throws IOException {
        withIndex(scenarioDocs(), ctx -> {
            var warnings = new MockWarnings();
            var loader = loaderFactory.apply(warnings);
            int expectedWarnings = 0;
            List<Integer> expectedLengths = new ArrayList<>();
            try (var reference = referenceReader(BREAKER, ctx); var reader = loader.reader(BREAKER, ctx)) {
                assertThat(reader, hasToString(expectedToString));
                BlockLoader.Docs docs = TestBlock.docs(ctx);
                try (TestBlock ref = read(reference, docs); TestBlock lengths = read(reader, docs)) {
                    for (int i = 0; i < ref.size(); i++) {
                        Object v = ref.get(i);
                        if (v == null || v instanceof List<?>) {
                            // missing / null-only / empty (null) or a genuine multi-value (single-value function warns and nulls)
                            assertThat(lengths.get(i), nullValue());
                            expectedLengths.add(null);
                            if (v instanceof List<?>) {
                                expectedWarnings++;
                            }
                        } else {
                            int expected = length.applyAsInt((BytesRef) v);
                            assertThat(lengths.get(i), equalTo(expected));
                            expectedLengths.add(expected);
                        }
                    }
                }
            }
            List<MockWarnings.MockWarning> expected = new ArrayList<>();
            for (int i = 0; i < expectedWarnings; i++) {
                expected.add(new MockWarnings.MockWarning(IllegalArgumentException.class, "single-value function encountered multi-value"));
            }
            assertThat(warnings.warnings(), equalTo(expected));

            // Single-doc reads take the count==1 constantInt / constantNulls branch, which the batch loop above never exercises. A fresh
            // reader per doc keeps the read at count==1 and its own warnings sink avoids double-counting the multi-value warnings.
            for (int i = 0; i < expectedLengths.size(); i++) {
                var singleWarnings = new MockWarnings();
                var singleLoader = loaderFactory.apply(singleWarnings);
                try (var reader = singleLoader.reader(BREAKER, ctx); TestBlock block = read(reader, TestBlock.docs(i))) {
                    assertThat(block.get(0), equalTo(expectedLengths.get(i)));
                }
            }
        });
    }

    private void checkExtreme(TestBlock reference, TestBlock extremes, Comparator<BytesRef> order) {
        for (int i = 0; i < reference.size(); i++) {
            Object v = reference.get(i);
            if (v == null) {
                assertThat(extremes.get(i), nullValue());
            } else if (v instanceof List<?> list) {
                BytesRef expected = list.stream().map(BytesRef.class::cast).max(order).orElseThrow();
                assertThat(extremes.get(i), equalTo(expected));
            } else {
                assertThat(extremes.get(i), equalTo(v));
            }
        }
    }
}
