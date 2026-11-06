/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.codec.profiler.ProfileCollector;
import org.elasticsearch.codec.profiler.TestCollectors;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class ProfiledStringBlockSinkTests extends ESTestCase {

    private static final class Recording implements StringBlockSink {
        int[] ordinals;
        int valueCount = -1;
        int[] valueCounts;
        int docCount = -1;
        BytesRef[] dictionary;
        int dictionarySize = -1;
        int appendOrdinalsCalls;
        int valuesCalls;
        final List<BytesRef> appended = new ArrayList<>();
        final Values returned = new Values() {
            @Override
            public void append(BytesRef value) {
                appended.add(value);
            }

            @Override
            public void finish() {}

            @Override
            public void close() {}
        };
        RuntimeException failure;

        @Override
        public void appendOrdinals(int[] o, int vc, int[] counts, int dc, BytesRef[] dict, int dictSize) {
            appendOrdinalsCalls++;
            if (failure != null) {
                throw failure;
            }
            ordinals = o;
            valueCount = vc;
            valueCounts = counts;
            docCount = dc;
            dictionary = dict;
            dictionarySize = dictSize;
        }

        @Override
        public Values values(int vc, int[] counts, int dc) {
            valuesCalls++;
            if (failure != null) {
                throw failure;
            }
            valueCount = vc;
            valueCounts = counts;
            docCount = dc;
            return returned;
        }
    }

    public void testOrdinalPagesReachTheDelegateWithTheSameArrays() {
        final Recording delegate = new Recording();
        final ProfileCollector stats = TestCollectors.elapsed();
        final StringBlockSink sink = new ProfiledStringBlockSink(delegate, new ProfiledStringBlockSink.Counters(stats));
        final int[] ordinals = { 0, 1, 1 };
        final int[] valueCounts = { 1, 1, 1 };
        final BytesRef[] dictionary = { new BytesRef("a"), new BytesRef("b") };

        sink.appendOrdinals(ordinals, 3, valueCounts, 3, dictionary, 2);

        assertThat(delegate.ordinals, sameInstance(ordinals));
        assertThat(delegate.valueCounts, sameInstance(valueCounts));
        assertThat(delegate.dictionary, sameInstance(dictionary));
        assertThat(delegate.valueCount, equalTo(3));
        assertThat(delegate.docCount, equalTo(3));
        assertThat(delegate.dictionarySize, equalTo(2));
        assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.pages.as_ordinals"), equalTo(1L));
        assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.pages.values_in_ordinal_pages"), equalTo(3L));
    }

    public void testValuePagesReturnTheDelegatesOwnValuesAndAppendNothingThemselves() throws IOException {
        final Recording delegate = new Recording();
        final ProfileCollector stats = TestCollectors.elapsed();
        final StringBlockSink sink = new ProfiledStringBlockSink(delegate, new ProfiledStringBlockSink.Counters(stats));
        final int[] valueCounts = { 1, 2 };

        final StringBlockSink.Values values = sink.values(3, valueCounts, 2);

        assertThat("no per value wrapper", values, sameInstance(delegate.returned));
        assertThat(delegate.valueCounts, sameInstance(valueCounts));
        values.append(new BytesRef("x"));
        assertThat(delegate.appended.size(), equalTo(1));
        assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.pages.as_values"), equalTo(1L));
        assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.pages.values_in_value_pages"), equalTo(3L));
    }

    public void testAnEmptyPageIsForwardedAndNotCounted() {
        final Recording delegate = new Recording();
        final ProfileCollector stats = TestCollectors.elapsed();
        final StringBlockSink sink = new ProfiledStringBlockSink(delegate, new ProfiledStringBlockSink.Counters(stats));

        sink.values(0, new int[0], 0);

        assertThat(delegate.valuesCalls, equalTo(1));
        assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.pages.as_values"), equalTo(0L));
        assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.pages.values_in_value_pages"), equalTo(0L));
    }

    public void testAFailingDelegatePropagatesItsExceptionAndTheSinkKeepsWorking() {
        final Recording delegate = new Recording();
        final ProfileCollector stats = TestCollectors.elapsed();
        final StringBlockSink sink = new ProfiledStringBlockSink(delegate, new ProfiledStringBlockSink.Counters(stats));
        delegate.failure = new IllegalStateException("consumer failed");

        final RuntimeException thrown = expectThrows(
            IllegalStateException.class,
            () -> sink.appendOrdinals(new int[] { 0 }, 1, new int[] { 1 }, 1, new BytesRef[] { new BytesRef("a") }, 1)
        );
        assertThat(thrown, sameInstance(delegate.failure));
        assertThat(
            "a page is counted when it begins, as the metric says",
            TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.pages.as_ordinals"),
            equalTo(1L)
        );

        delegate.failure = null;
        sink.values(2, new int[] { 1, 1 }, 2);
        assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.pages.as_values"), equalTo(1L));
        assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.pages.as_ordinals"), equalTo(1L));
    }

    public void testTwoSinksOnOneReaderShareTheirCountersAndSeparateSinksDoNotMix() {
        final ProfileCollector stats = TestCollectors.elapsed();
        final ProfiledStringBlockSink.Counters counters = new ProfiledStringBlockSink.Counters(stats);
        final Recording first = new Recording();
        final Recording second = new Recording();
        new ProfiledStringBlockSink(first, counters).values(1, new int[] { 1 }, 1);
        new ProfiledStringBlockSink(second, counters).values(1, new int[] { 1 }, 1);
        assertThat(first.valuesCalls, equalTo(1));
        assertThat(second.valuesCalls, equalTo(1));
        assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.pages.as_values"), equalTo(2L));
    }
}
