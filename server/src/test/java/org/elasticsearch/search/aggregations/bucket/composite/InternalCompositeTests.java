/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.test.InternalMultiBucketAggregationTestCase;
import org.junit.After;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomAsciiLettersOfLengthBetween;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class InternalCompositeTests extends InternalMultiBucketAggregationTestCase<InternalComposite> {
    private List<String> sourceNames;
    private List<DocValueFormat> formats;
    private int[] reverseMuls;
    private int[] types;
    private int size;

    private static DocValueFormat randomDocValueFormat(boolean isLong) {
        if (isLong) {
            // we use specific format only for date histogram on a long/date field
            if (randomBoolean()) {
                return new DocValueFormat.DateTime(DateFormatter.forPattern("epoch_second"), ZoneOffset.ofHours(1),
                    DateFieldMapper.Resolution.MILLISECONDS);
            } else {
                return DocValueFormat.RAW;
            }
        } else {
            // and the raw format for the other types
            return DocValueFormat.RAW;
        }
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        int numFields = randomIntBetween(1, 10);
        size = randomNumberOfBuckets();
        sourceNames = new ArrayList<>();
        formats = new ArrayList<>();
        reverseMuls = new int[numFields];
        types = new int[numFields];
        for (int i = 0; i < numFields; i++) {
            sourceNames.add("field_" + i);
            reverseMuls[i] = randomBoolean() ? 1 : -1;
            int type = randomIntBetween(0, 2);
            types[i] = type;
            formats.add(randomDocValueFormat(type == 0));
        }
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        sourceNames = null;
        formats = null;
        reverseMuls = null;
        types = null;
    }

    @Override
    protected Class<ParsedComposite> implementationClass() {
        return ParsedComposite.class;
    }

    protected <P extends ParsedAggregation> P parseAndAssert(final InternalAggregation aggregation,
                                                             final boolean shuffled, final boolean addRandomFields) throws IOException {
        return super.parseAndAssert(aggregation, false, false);
    }

    private CompositeKey createCompositeKey() {
        Comparable<?>[] keys = new Comparable<?>[sourceNames.size()];
        for (int j = 0; j  < keys.length; j++) {
            switch (types[j]) {
                case 0:
                    keys[j] = randomLong();
                    break;
                case 1:
                    keys[j] = randomDouble();
                    break;
                case 2:
                    keys[j] = new BytesRef(randomAsciiLettersOfLengthBetween(1, 20));
                    break;
                default:
                    throw new AssertionError("illegal branch");
            }
        }
        return new CompositeKey(keys);
    }

    @SuppressWarnings("unchecked")
    private Comparator<CompositeKey> getKeyComparator() {
        return (o1, o2) -> {
            for (int i = 0; i < o1.size(); i++) {
                int cmp = o1.get(i).compareTo(o2.get(i)) * reverseMuls[i];
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        };
    }

    @Override
    protected InternalComposite createTestInstance(String name, Map<String, Object> metadata, InternalAggregations aggregations) {
        int numBuckets = randomIntBetween(0, size);
        List<InternalComposite.InternalBucket> buckets = new ArrayList<>();
        TreeSet<CompositeKey> keys = new TreeSet<>(getKeyComparator());
        for (int i = 0;  i < numBuckets; i++) {
            final CompositeKey key = createCompositeKey();
            if (keys.contains(key)) {
                continue;
            }
            keys.add(key);
            InternalComposite.InternalBucket bucket =
                new InternalComposite.InternalBucket(sourceNames, formats, key, reverseMuls, 1L, aggregations);
            buckets.add(bucket);
        }
        Collections.sort(buckets, (o1, o2) -> o1.compareKey(o2));
        CompositeKey lastBucket = buckets.size() > 0 ? buckets.get(buckets.size()-1).getRawKey() : null;
        return new InternalComposite(name, size, sourceNames, formats, buckets, lastBucket, reverseMuls, randomBoolean(), metadata);
    }

    @Override
    protected InternalComposite mutateInstance(InternalComposite instance) throws IOException {
        List<InternalComposite.InternalBucket> buckets = instance.getBuckets();
        Map<String, Object> metadata = instance.getMetadata();
        int code = randomIntBetween(0, 2);
        int[] reverseMuls = instance.getReverseMuls();
        switch(code) {
            case 0:
                int[] newReverseMuls = new int[reverseMuls.length];
                for (int i = 0; i < reverseMuls.length; i++) {
                    newReverseMuls[i] = reverseMuls[i] == 1 ? -1 : 1;
                }
                reverseMuls = newReverseMuls;
                break;
            case 1:
                buckets = new ArrayList<>(buckets);
                buckets.add(new InternalComposite.InternalBucket(sourceNames, formats, createCompositeKey(), reverseMuls,
                    randomLongBetween(1, 100), InternalAggregations.EMPTY)
                );
                break;
            case 2:
                if (metadata == null) {
                    metadata = new HashMap<>(1);
                } else {
                    metadata = new HashMap<>(instance.getMetadata());
                }
                metadata.put(randomAlphaOfLength(15), randomInt());
                break;
            default:
                throw new AssertionError("illegal branch");
        }
        CompositeKey lastBucket = buckets.size() > 0 ? buckets.get(buckets.size()-1).getRawKey() : null;
        return new InternalComposite(instance.getName(), instance.getSize(), sourceNames, formats, buckets, lastBucket, reverseMuls,
            randomBoolean(), metadata);
    }

    @Override
    protected void assertReduced(InternalComposite reduced, List<InternalComposite> inputs) {
        List<CompositeKey> expectedKeys = inputs.stream()
            .flatMap((s) -> s.getBuckets().stream())
            .map(InternalComposite.InternalBucket::getRawKey)
            .sorted(getKeyComparator())
            .distinct()
            .limit(reduced.getSize())
            .collect(Collectors.toList());

        assertThat(reduced.getBuckets().size(), lessThanOrEqualTo(size));
        assertThat(reduced.getBuckets().size(), equalTo(expectedKeys.size()));
        Iterator<CompositeKey> expectedIt = expectedKeys.iterator();
        for (InternalComposite.InternalBucket bucket : reduced.getBuckets()) {
            assertTrue(expectedIt.hasNext());
            assertThat(bucket.getRawKey(), equalTo(expectedIt.next()));
        }
        assertFalse(expectedIt.hasNext());
    }

    public void testReduceSame() throws IOException {
        InternalComposite result = createTestInstance(randomAlphaOfLength(10), Collections.emptyMap(),
            InternalAggregations.EMPTY);
        List<InternalAggregation> toReduce = new ArrayList<>();
        int numSame = randomIntBetween(1, 10);
        for (int i = 0; i < numSame; i++) {
            toReduce.add(result);
        }
        InternalComposite finalReduce = (InternalComposite) result.reduce(toReduce, emptyReduceContextBuilder().forFinalReduction());
        assertThat(finalReduce.getBuckets().size(), equalTo(result.getBuckets().size()));
        Iterator<InternalComposite.InternalBucket> expectedIt = result.getBuckets().iterator();
        for (InternalComposite.InternalBucket bucket : finalReduce.getBuckets()) {
            InternalComposite.InternalBucket expectedBucket = expectedIt.next();
            assertThat(bucket.getRawKey(), equalTo(expectedBucket.getRawKey()));
            assertThat(bucket.getDocCount(), equalTo(expectedBucket.getDocCount()*numSame));
        }
    }

    /**
     * Check that reducing with an unmapped index produces useful formats.
     */
    public void testReduceUnmapped() throws IOException {
        var mapped = createTestInstance(randomAlphaOfLength(10), emptyMap(), InternalAggregations.EMPTY);
        var rawFormats = formats.stream().map(f -> DocValueFormat.RAW).collect(toList());
        var unmapped = new InternalComposite(mapped.getName(), mapped.getSize(), sourceNames,
                rawFormats, emptyList(), null, reverseMuls, true, emptyMap());
        List<InternalAggregation> toReduce = Arrays.asList(unmapped, mapped);
        Collections.shuffle(toReduce, random());
        InternalComposite finalReduce = (InternalComposite) unmapped.reduce(toReduce, emptyReduceContextBuilder().forFinalReduction());
        assertThat(finalReduce.getBuckets().size(), equalTo(mapped.getBuckets().size()));
        if (false == mapped.getBuckets().isEmpty()) {
            assertThat(finalReduce.getFormats(), equalTo(mapped.getFormats()));
        }
        var expectedIt = mapped.getBuckets().iterator();
        for (var bucket : finalReduce.getBuckets()) {
            InternalComposite.InternalBucket expectedBucket = expectedIt.next();
            assertThat(bucket.getRawKey(), equalTo(expectedBucket.getRawKey()));
            assertThat(bucket.getDocCount(), equalTo(expectedBucket.getDocCount()));
            assertThat(bucket.getFormats(), equalTo(expectedBucket.getFormats()));
        }
    }

    public void testCompareCompositeKeyBiggerFieldName() {
        InternalComposite.ArrayMap key1 = createMap(
            Arrays.asList("field1", "field2"),
            new Comparable<?>[]{1, 2}
        );
        InternalComposite.ArrayMap key2 = createMap(
            Arrays.asList("field3", "field2"),
            new Comparable<?>[]{1, 2}
        );
        assertThat(key1.compareTo(key2), lessThan(0));
    }

    public void testCompareCompositeKeySmallerFieldName() {
        InternalComposite.ArrayMap key1 = createMap(
            Arrays.asList("field3", "field2"),
            new Comparable<?>[]{1, 2}
        );
        InternalComposite.ArrayMap key2 = createMap(
            Arrays.asList("field1", "field2"),
            new Comparable<?>[]{1, 2}
        );
        assertThat(key1.compareTo(key2), greaterThan(0));
    }

    public void testCompareCompositeKeyBiggerValue() {
        InternalComposite.ArrayMap key1 = createMap(
            Arrays.asList("field1", "field2"),
            new Comparable<?>[]{1, 2}
        );
        InternalComposite.ArrayMap key2 = createMap(
            Arrays.asList("field3", "field2"),
            new Comparable<?>[]{2, 3}
        );
        assertThat(key1.compareTo(key2), lessThan(0));
    }

    public void testCompareCompositeKeySmallerValue() {
        InternalComposite.ArrayMap key1 = createMap(
            Arrays.asList("field3", "field2"),
            new Comparable<?>[]{1, 2}
        );
        InternalComposite.ArrayMap key2 = createMap(
            Arrays.asList("field1", "field2"),
            new Comparable<?>[]{2, 3}
        );
        assertThat(key1.compareTo(key2), greaterThan(0));
    }

    public void testCompareCompositeKeyNullValueIsSmaller1() {
        InternalComposite.ArrayMap key1 = createMap(
            Arrays.asList("field1", "field2"),
            new Comparable<?>[]{null, 20}
        );
        InternalComposite.ArrayMap key2 = createMap(
            Arrays.asList("field1", "field2"),
            new Comparable<?>[]{1, 2}
        );
        assertThat(key1.compareTo(key2), lessThan(0));
    }

    public void testCompareCompositeKeyNullValueIsSmaller2() {
        InternalComposite.ArrayMap key1 = createMap(
            Arrays.asList("field1", "field2"),
            new Comparable<?>[]{1, 2}
        );
        InternalComposite.ArrayMap key2 = createMap(
            Arrays.asList("field1", "field2"),
            new Comparable<?>[]{null, 20}
        );
        assertThat(key1.compareTo(key2), greaterThan(0));
    }

    public void testCompareCompositeKeyMoreFieldsIsGreater() {
        InternalComposite.ArrayMap key1 = createMap(
            Arrays.asList("field1", "field2"),
            new Comparable<?>[]{1, 2}
        );
        InternalComposite.ArrayMap key2 = createMap(Arrays.asList("field1", "field2", "field3"),new Comparable<?>[]{1, 2, null});
        assertThat(key1.compareTo(key2), lessThan(0));
    }

    public void testCompareCompositeKeyLessFieldsIsLesser() {
        InternalComposite.ArrayMap key1 = createMap(
            Arrays.asList("field1", "field2", "field3"),
            new Comparable<?>[]{1, 2, null}
        );
        InternalComposite.ArrayMap key2 = createMap(Arrays.asList("field1", "field2"),new Comparable<?>[]{1, 2});
        assertThat(key1.compareTo(key2), greaterThan(0));
    }

    public void testCompareCompositeKeyEqual() {
        InternalComposite.ArrayMap key1 = createMap(
            Arrays.asList("field1", "field2", "field3"),
            new Comparable<?>[]{null, 1, 2}
        );
        InternalComposite.ArrayMap key2 = createMap(
            Arrays.asList("field1", "field2", "field3"),
            new Comparable<?>[]{null, 1, 2}
        );
        assertThat(key1.compareTo(key1), equalTo(0));
        assertThat(key1.equals(key1), is(true));

        assertThat(key1.compareTo(key2), equalTo(0));
        assertThat(key1.equals(key2), is(true));
        assertThat(key2.equals(key1), is(true));
    }

    public void testCompareCompositeKeyValuesHaveDifferentTypes() {
        InternalComposite.ArrayMap key1 = createMap(
            Arrays.asList("field1", "field2"),
            new Comparable<?>[]{1, 2}
        );

        InternalComposite.ArrayMap key2 = createMap(
            Arrays.asList("field1", "field2"),
            new Comparable<?>[]{"1", 2}
        );

        ClassCastException exception = expectThrows(ClassCastException.class, () -> key1.compareTo(key2));
        assertThat(exception.getMessage(),
            containsString("java.lang.String cannot be cast to"));
    }

    public void testFormatObjectChecked() {
        DocValueFormat weekYearMonth = new DocValueFormat.DateTime(
            // YYYY is week-based year.  The parser will ignore MM (month-of-year) and dd (day-of-month)
            DateFormatter.forPattern("YYYY-MM-dd"),
            ZoneOffset.UTC,
            DateFieldMapper.Resolution.MILLISECONDS
        );
        long epoch = 1622060077L; // May 25th 2021, evening
        expectThrows(IllegalArgumentException.class, () -> InternalComposite.formatObject(epoch, weekYearMonth));
    }

    public void testFormatDateEpochTimezone() {
        DocValueFormat epochSecond = new DocValueFormat.DateTime(
            // YYYY is week-based year.  The parser will ignore MM (month-of-year) and dd (day-of-month)
            DateFormatter.forPattern("epoch_second"),
            ZoneOffset.ofHours(-2),
            DateFieldMapper.Resolution.MILLISECONDS
        );
        DocValueFormat epochMillis = new DocValueFormat.DateTime(
            // YYYY is week-based year.  The parser will ignore MM (month-of-year) and dd (day-of-month)
            DateFormatter.forPattern("epoch_millis"),
            ZoneOffset.ofHours(-2),
            DateFieldMapper.Resolution.MILLISECONDS
        );
        long epoch = 1622060077L; // May 25th 2021, evening
        Object actual = InternalComposite.formatObject(epoch, epochSecond);
        assertEquals("1622060.077", actual.toString());

        actual = InternalComposite.formatObject(epoch, epochMillis);
        assertEquals("1622060077", actual.toString());
    }

    private InternalComposite.ArrayMap createMap(List<String> fields, Comparable<?>[] values) {
        List<DocValueFormat> formats = IntStream.range(0, fields.size())
            .mapToObj(i -> DocValueFormat.RAW).collect(Collectors.toList());
        return new InternalComposite.ArrayMap(
            fields,
            formats,
            values
        );
    }
}
