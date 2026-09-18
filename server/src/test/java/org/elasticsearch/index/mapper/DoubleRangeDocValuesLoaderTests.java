/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License, v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.mapper.blockloader.docvalues.DoubleRangeDocValuesLoader;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

/**
 * Tests loading double range binary doc values into ES|QL's half-open range representation.
 */
public class DoubleRangeDocValuesLoaderTests extends ESTestCase {

    private static final String FIELD_NAME = "test_double_range";

    public void testLoadsSingleAndMultipleRanges() throws IOException {
        var singleRange = BinaryRangeUtil.encodeDoubleRanges(
            Set.of(new RangeFieldMapper.Range(RangeType.DOUBLE, 1.5, Math.nextDown(2.0), true, true))
        );
        var multipleRanges = BinaryRangeUtil.encodeDoubleRanges(
            Set.of(
                new RangeFieldMapper.Range(RangeType.DOUBLE, 10.0, Math.nextDown(20.0), true, true),
                new RangeFieldMapper.Range(RangeType.DOUBLE, 30.0, Math.nextDown(40.0), true, true)
            )
        );

        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            iw.addDocument(List.of(new BinaryDocValuesField(FIELD_NAME, singleRange)));
            iw.addDocument(List.of(new BinaryDocValuesField(FIELD_NAME, multipleRanges)));
            iw.forceMerge(1);

            try (DirectoryReader dr = iw.getReader()) {
                var context = getOnlyLeafReader(dr).getContext();
                var loader = new DoubleRangeDocValuesLoader(FIELD_NAME);
                try (var reader = loader.columnAtATimeReader(context).apply(newLimitedBreaker(ByteSizeValue.ofMb(1)))) {
                    var block = (TestBlock) reader.read(TestBlock.factory(), TestBlock.docs(0, 1), 0, false);

                    assertThat(block.size(), equalTo(2));
                    assertThat(List.of(block.get(0), block.get(1)), hasItem(List.of(List.of(10.0, 30.0), List.of(20.0, 40.0))));
                    assertThat(List.of(block.get(0), block.get(1)), hasItem(List.of(1.5, 2.0)));
                }
            }
        }
    }

    public void testEmptyAndMissingRangesAppendNull() throws IOException {
        var oneRange = BinaryRangeUtil.encodeDoubleRanges(
            Set.of(new RangeFieldMapper.Range(RangeType.DOUBLE, 1.0, Math.nextDown(2.0), true, true))
        );
        var emptyRanges = BinaryRangeUtil.encodeDoubleRanges(Set.of());

        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            iw.addDocument(List.of(new BinaryDocValuesField(FIELD_NAME, oneRange)));
            iw.addDocument(List.of(new BinaryDocValuesField(FIELD_NAME, emptyRanges)));
            iw.addDocument(List.of());
            iw.forceMerge(1);

            try (DirectoryReader dr = iw.getReader()) {
                var context = getOnlyLeafReader(dr).getContext();
                var loader = new DoubleRangeDocValuesLoader(FIELD_NAME);
                try (var reader = loader.columnAtATimeReader(context).apply(newLimitedBreaker(ByteSizeValue.ofMb(1)))) {
                    var block = (TestBlock) reader.read(TestBlock.factory(), TestBlock.docs(0, 1, 2), 0, false);

                    assertThat(block.size(), equalTo(3));
                    List<Object> values = Arrays.asList(block.get(0), block.get(1), block.get(2));
                    assertThat(values, hasItem(List.of(1.0, 2.0)));
                    assertThat(values.stream().filter(Objects::isNull).count(), equalTo(2L));
                }
            }
        }
    }
}
