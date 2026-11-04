/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.test.InternalMultiBucketAggregationTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.junit.After;
import org.junit.Before;

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
import static org.hamcrest.Matchers.not;

public class InternalCompositeTests extends InternalMultiBucketAggregationTestCase<InternalComposite> {
    private List<String> sourceNames;
    private List<DocValueFormat> formats;
    private int[] reverseMuls;
    private MissingOrder[] missingOrders;
    private int[] types;
    private int size;

    private static DocValueFormat randomDocValueFormat(boolean isLong) {
        if (isLong) {
            // we use specific format only for date histogram on a long/date field
            if (randomBoolean()) {
                return new DocValueFormat.DateTime(
                    DateFormatter.forPattern("epoch_second"),
                    ZoneOffset.ofHours(1),
                    DateFieldMapper.Resolution.MILLISECONDS
                );
            } else {
                return DocValueFormat.RAW;
            }
        } else {
            // and the raw format for the other types
            return DocValueFormat.RAW;
        }
    }

    @Override
    protected boolean supportsSampling() {
        return true;
    }

    @Before
    public void initializeCompositeFields() throws Exception {
        int numFields = randomIntBetween(1, 10);
        size = randomNumberOfBuckets();
        sourceNames = new ArrayList<>();
        formats = new ArrayList<>();
        reverseMuls = new int[numFields];
        missingOrders = new MissingOrder[numFields];
        types = new int[numFields];
        for (int i = 0; i < numFields; i++) {
            sourceNames.add("field_" + i);
            reverseMuls[i] = randomBoolean() ? 1 : -1;
            missingOrders[i] = randomFrom(MissingOrder.values());
            int type = randomIntBetween(0, 2);
            types[i] = type;
            formats.add(randomDocValueFormat(type == 0));
        }
    }

    @After
    public void clearCompositeFields() throws Exception {
        sourceNames = null;
        formats = null;
        reverseMuls = null;
        missingOrders = null;
        types = null;
    }

    private CompositeKey createCompositeKey() {
        Comparable<?>[] keys = new Comparable<?>[sourceNames.size()];
        for (int j = 0; j < keys.length; j++) {
            switch (types[j]) {
                case 0 -> keys[j] = randomLong();
                case 1 -> keys[j] = randomDouble();
                case 2 -> keys[j] = new BytesRef(randomAsciiLettersOfLengthBetween(1, 20));
                default -> throw new AssertionError("illegal branch");
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
        for (int i = 0; i < numBuckets; i++) {
            final CompositeKey key = createCompositeKey();
            if (keys.contains(key)) {
                continue;
            }
            keys.add(key);
            InternalComposite.InternalBucket bucket = new InternalComposite.InternalBucket(sourceNames, formats, key, 1L, aggregations);
            buckets.add(bucket);
        }
        Collections.sort(buckets, (o1, o2) -> o1.compareKey(o2, reverseMuls, missingOrders));
        CompositeKey lastBucket = buckets.size() > 0 ? buckets.get(buckets.size() - 1).getRawKey() : null;
        return new InternalComposite(
            name,
            size,
            sourceNames,
            formats,
            buckets,
            lastBucket,
            reverseMuls,
            missingOrders,
            randomBoolean(),
            metadata
        );
    }

    @Override
    protected InternalComposite mutateInstance(InternalComposite instance) {
        List<InternalComposite.InternalBucket> buckets = instance.getBuckets();
        Map<String, Object> metadata = instance.getMetadata();
        int code = randomIntBetween(0, 2);
        int[] reverseMuls = instance.getReverseMuls();
        switch (code) {
            case 0 -> {
                int[] newReverseMuls = new int[reverseMuls.length];
                for (int i = 0; i < reverseMuls.length; i++) {
                    newReverseMuls[i] = reverseMuls[i] == 1 ? -1 : 1;
                }
                reverseMuls = newReverseMuls;
            }
            case 1 -> {
                buckets = new ArrayList<>(buckets);
                buckets.add(
                    new InternalComposite.InternalBucket(
                        sourceNames,
                        formats,
                        createCompositeKey(),
                        randomLongBetween(1, 100),
                        InternalAggregations.EMPTY
                    )
                );
            }
            case 2 -> {
                if (metadata == null) {
                    metadata = Maps.newMapWithExpectedSize(1);
                } else {
                    metadata = new HashMap<>(instance.getMetadata());
                }
                metadata.put(randomAlphaOfLength(15), randomInt());
            }
            default -> throw new AssertionError("illegal branch");
        }
        CompositeKey lastBucket = buckets.size() > 0 ? buckets.get(buckets.size() - 1).getRawKey() : null;
        return new InternalComposite(
            instance.getName(),
            instance.getSize(),
            sourceNames,
            formats,
            buckets,
            lastBucket,
            reverseMuls,
            missingOrders,
            randomBoolean(),
            metadata
        );
    }

    @Override
    protected void assertReduced(InternalComposite reduced, List<InternalComposite> inputs) {
        List<CompositeKey> expectedKeys = inputs.stream()
            .flatMap((s) -> s.getBuckets().stream())
            .map(InternalComposite.InternalBucket::getRawKey)
            .sorted(getKeyComparator())
            .distinct()
            .limit(reduced.getSize())
            .toList();

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
        InternalComposite result = createTestInstance(randomAlphaOfLength(10), Collections.emptyMap(), InternalAggregations.EMPTY);
        List<InternalAggregation> toReduce = new ArrayList<>();
        int numSame = randomIntBetween(1, 10);
        for (int i = 0; i < numSame; i++) {
            toReduce.add(result);
        }
        InternalComposite finalReduce = (InternalComposite) InternalAggregationTestCase.reduce(
            toReduce,
            emptyReduceContextBuilder().forFinalReduction()
        );
        assertThat(finalReduce.getBuckets().size(), equalTo(result.getBuckets().size()));
        Iterator<InternalComposite.InternalBucket> expectedIt = result.getBuckets().iterator();
        for (InternalComposite.InternalBucket bucket : finalReduce.getBuckets()) {
            InternalComposite.InternalBucket expectedBucket = expectedIt.next();
            assertThat(bucket.getRawKey(), equalTo(expectedBucket.getRawKey()));
            assertThat(bucket.getDocCount(), equalTo(expectedBucket.getDocCount() * numSame));
        }
    }

    /**
     * Check that reducing with an unmapped index produces useful formats.
     */
    public void testReduceUnmapped() throws IOException {
        var mapped = createTestInstance(randomAlphaOfLength(10), emptyMap(), InternalAggregations.EMPTY);
        var rawFormats = formats.stream().map(f -> DocValueFormat.RAW).collect(toList());
        var unmapped = new InternalComposite(
            mapped.getName(),
            mapped.getSize(),
            sourceNames,
            rawFormats,
            emptyList(),
            null,
            reverseMuls,
            missingOrders,
            true,
            emptyMap()
        );
        List<InternalAggregation> toReduce = Arrays.asList(unmapped, mapped);
        Collections.shuffle(toReduce, random());
        InternalComposite finalReduce = (InternalComposite) InternalAggregationTestCase.reduce(
            toReduce,
            emptyReduceContextBuilder().forFinalReduction()
        );
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

    /**
     * A partial reduce (e.g. on a data node with batched query phase) of a mapped and an unmapped index may produce buckets with
     * different formats. Formats are written once for the whole composite, so they must still format the keys of all the buckets
     * after a serialization round trip, even if the last bucket (here a missing bucket) comes from the unmapped index.
     */
    public void testPartialReduceMixedFormatsSurvivesSerialization() throws IOException {
        final String name = randomAlphaOfLength(10);
        final List<String> names = List.of("ip");
        final int[] muls = new int[] { 1 };
        final MissingOrder[] orders = new MissingOrder[] { MissingOrder.LAST };
        final String ipValue = "4.42.218.30";
        final BytesRef ipKey = new BytesRef(InetAddressPoint.encode(InetAddresses.forString(ipValue)));

        var mapped = new InternalComposite(
            name,
            10,
            names,
            List.of(DocValueFormat.IP),
            List.of(
                new InternalComposite.InternalBucket(
                    names,
                    List.of(DocValueFormat.IP),
                    new CompositeKey(ipKey),
                    1,
                    InternalAggregations.EMPTY
                )
            ),
            new CompositeKey(ipKey),
            muls,
            orders,
            false,
            emptyMap()
        );
        var missingKey = new CompositeKey(new Comparable<?>[] { null });
        var unmapped = new InternalComposite(
            name,
            10,
            names,
            List.of(DocValueFormat.RAW),
            List.of(new InternalComposite.InternalBucket(names, List.of(DocValueFormat.RAW), missingKey, 1, InternalAggregations.EMPTY)),
            missingKey,
            muls,
            orders,
            false,
            emptyMap()
        );

        List<InternalAggregation> toReduce = Arrays.asList(mapped, unmapped);
        Collections.shuffle(toReduce, random());
        InternalComposite partial = (InternalComposite) InternalAggregationTestCase.reduce(
            toReduce,
            emptyReduceContextBuilder().forPartialReduction()
        );
        assertThat(partial.getBuckets().size(), equalTo(2));
        assertThat(partial.getBuckets().get(1).getRawKey(), equalTo(missingKey));

        // The hop from the data node to the coordinator
        InternalComposite wire = (InternalComposite) copyNamedWriteable(partial, getNamedWriteableRegistry(), InternalAggregation.class);
        assertThat(wire.getBuckets().get(0).getKey().get("ip"), equalTo(ipValue));
        assertNull(wire.getBuckets().get(1).getKey().get("ip"));

        InternalComposite finalReduce = (InternalComposite) InternalAggregationTestCase.reduce(
            List.of(wire),
            emptyReduceContextBuilder().forFinalReduction()
        );
        assertThat(finalReduce.getBuckets().get(0).getKey().get("ip"), equalTo(ipValue));
        assertNull(finalReduce.getBuckets().get(1).getKey().get("ip"));
    }

    private static InternalComposite dateComposite(String name, DocValueFormat format, long key) {
        final List<String> names = List.of("date");
        final var bucketKey = new CompositeKey(key);
        return new InternalComposite(
            name,
            10,
            names,
            List.of(format),
            List.of(new InternalComposite.InternalBucket(names, List.of(format), bucketKey, 1, InternalAggregations.EMPTY)),
            bucketKey,
            new int[] { 1 },
            new MissingOrder[] { MissingOrder.DEFAULT },
            false,
            emptyMap()
        );
    }

    /**
     * Buckets with non-null keys that really have different formats (here dates of indices with different formats) must keep their
     * own format after a serialization round trip, and still use a single format list if the node on the other side is older.
     */
    public void testPartialReduceDifferentNonNullFormatsSurvivesSerialization() throws IOException {
        final String name = randomAlphaOfLength(10);
        final var epochSecond = new DocValueFormat.DateTime(
            DateFormatter.forPattern("epoch_second"),
            ZoneOffset.UTC,
            DateFieldMapper.Resolution.MILLISECONDS
        );
        final var isoDate = new DocValueFormat.DateTime(
            DateFormatter.forPattern("strict_date_optional_time"),
            ZoneOffset.UTC,
            DateFieldMapper.Resolution.MILLISECONDS
        );
        final var first = dateComposite(name, epochSecond, 1_000L);
        final var second = dateComposite(name, isoDate, 2_000L);
        final String expectedFirst = epochSecond.format(1_000L).toString();
        final String expectedSecond = isoDate.format(2_000L).toString();
        assertThat(expectedFirst, not(equalTo(expectedSecond)));

        List<InternalAggregation> toReduce = Arrays.asList(first, second);
        Collections.shuffle(toReduce, random());
        InternalComposite partial = (InternalComposite) InternalAggregationTestCase.reduce(
            toReduce,
            emptyReduceContextBuilder().forPartialReduction()
        );
        assertThat(partial.getBuckets().size(), equalTo(2));

        InternalComposite wire = (InternalComposite) copyNamedWriteable(partial, getNamedWriteableRegistry(), InternalAggregation.class);
        assertThat(wire.getBuckets().get(0).getKey().get("date"), equalTo(expectedFirst));
        assertThat(wire.getBuckets().get(1).getKey().get("date"), equalTo(expectedSecond));
        assertThat(wire.getBuckets().get(0).getFormats(), equalTo(List.of(epochSecond)));
        assertThat(wire.getBuckets().get(1).getFormats(), equalTo(List.of(isoDate)));

        // With a node that doesn't know about per bucket formats, we write the formats of the composite only
        InternalComposite oldWire = (InternalComposite) copyNamedWriteable(
            partial,
            getNamedWriteableRegistry(),
            InternalAggregation.class,
            TransportVersionUtils.getPreviousVersion(InternalComposite.COMPOSITE_PER_BUCKET_FORMATS)
        );
        assertThat(oldWire.getBuckets().size(), equalTo(2));
        assertThat(oldWire.getBuckets().get(0).getFormats(), equalTo(oldWire.getBuckets().get(1).getFormats()));
    }

    /**
     * Only the buckets whose formats differ from the ones of the composite are serialized, identified by their position. Mixes
     * conforming buckets (same format as the composite, or a null key which is never formatted) with buckets of another format, so
     * that each bucket gets back its own formats.
     */
    public void testSerializeSparseBucketFormats() throws IOException {
        final String name = randomAlphaOfLength(10);
        final List<String> names = List.of("date");
        final var epochSecond = new DocValueFormat.DateTime(
            DateFormatter.forPattern("epoch_second"),
            ZoneOffset.UTC,
            DateFieldMapper.Resolution.MILLISECONDS
        );
        final var isoDate = new DocValueFormat.DateTime(
            DateFormatter.forPattern("strict_date_optional_time"),
            ZoneOffset.UTC,
            DateFieldMapper.Resolution.MILLISECONDS
        );
        // Formats of the composite are isoDate: buckets 1 and 3 conform, 0 and 2 don't, and 4 has a null key so it always conforms
        final List<List<DocValueFormat>> bucketFormats = List.of(
            List.of(epochSecond),
            List.of(isoDate),
            List.of(epochSecond),
            List.of(isoDate),
            List.of(DocValueFormat.RAW)
        );
        final List<InternalComposite.InternalBucket> buckets = new ArrayList<>();
        for (int i = 0; i < bucketFormats.size(); i++) {
            final var key = new CompositeKey(i == bucketFormats.size() - 1 ? null : 1_000L * (i + 1));
            buckets.add(new InternalComposite.InternalBucket(names, bucketFormats.get(i), key, 1, InternalAggregations.EMPTY));
        }
        final var composite = new InternalComposite(
            name,
            10,
            names,
            List.of(isoDate),
            buckets,
            buckets.get(buckets.size() - 1).getRawKey(),
            new int[] { 1 },
            new MissingOrder[] { MissingOrder.LAST },
            false,
            emptyMap()
        );

        InternalComposite wire = (InternalComposite) copyNamedWriteable(composite, getNamedWriteableRegistry(), InternalAggregation.class);
        assertThat(wire.getBuckets().size(), equalTo(buckets.size()));
        for (int i = 0; i < buckets.size(); i++) {
            final var expected = i == buckets.size() - 1 ? List.of(isoDate) : bucketFormats.get(i);
            assertThat("formats of bucket " + i, wire.getBuckets().get(i).getFormats(), equalTo(expected));
            assertThat("key of bucket " + i, wire.getBuckets().get(i).getKey(), equalTo(composite.getBuckets().get(i).getKey()));
        }
    }

    /**
     * The mapped / unmapped case that is fixed by selecting the formats must also work when talking to an older node.
     */
    public void testPartialReduceMixedFormatsOldTransportVersion() throws IOException {
        final String name = randomAlphaOfLength(10);
        final List<String> names = List.of("ip");
        final BytesRef ipKey = new BytesRef(InetAddressPoint.encode(InetAddresses.forString("4.42.218.30")));
        final var missingKey = new CompositeKey(new Comparable<?>[] { null });
        var mapped = new InternalComposite(
            name,
            10,
            names,
            List.of(DocValueFormat.IP),
            List.of(
                new InternalComposite.InternalBucket(
                    names,
                    List.of(DocValueFormat.IP),
                    new CompositeKey(ipKey),
                    1,
                    InternalAggregations.EMPTY
                )
            ),
            new CompositeKey(ipKey),
            new int[] { 1 },
            new MissingOrder[] { MissingOrder.LAST },
            false,
            emptyMap()
        );
        var unmapped = new InternalComposite(
            name,
            10,
            names,
            List.of(DocValueFormat.RAW),
            List.of(new InternalComposite.InternalBucket(names, List.of(DocValueFormat.RAW), missingKey, 1, InternalAggregations.EMPTY)),
            missingKey,
            new int[] { 1 },
            new MissingOrder[] { MissingOrder.LAST },
            false,
            emptyMap()
        );
        InternalComposite partial = (InternalComposite) InternalAggregationTestCase.reduce(
            List.of(mapped, unmapped),
            emptyReduceContextBuilder().forPartialReduction()
        );
        InternalComposite oldWire = (InternalComposite) copyNamedWriteable(
            partial,
            getNamedWriteableRegistry(),
            InternalAggregation.class,
            TransportVersionUtils.getPreviousVersion(InternalComposite.COMPOSITE_PER_BUCKET_FORMATS)
        );
        assertThat(oldWire.getBuckets().get(0).getKey().get("ip"), equalTo("4.42.218.30"));
        assertNull(oldWire.getBuckets().get(1).getKey().get("ip"));
    }

    /**
     * Without buckets there is nothing to pick formats from, the default ones are returned.
     */
    public void testFormatsForBucketsEmpty() {
        assertThat(InternalComposite.formatsForBuckets(emptyList(), List.of(DocValueFormat.IP)), equalTo(List.of(DocValueFormat.IP)));
    }

    public void testCompareCompositeKeyBiggerFieldName() {
        InternalComposite.ArrayMap key1 = createMap(Arrays.asList("field1", "field2"), new Comparable<?>[] { 1, 2 });
        InternalComposite.ArrayMap key2 = createMap(Arrays.asList("field3", "field2"), new Comparable<?>[] { 1, 2 });
        assertThat(key1.compareTo(key2), lessThan(0));
    }

    public void testCompareCompositeKeySmallerFieldName() {
        InternalComposite.ArrayMap key1 = createMap(Arrays.asList("field3", "field2"), new Comparable<?>[] { 1, 2 });
        InternalComposite.ArrayMap key2 = createMap(Arrays.asList("field1", "field2"), new Comparable<?>[] { 1, 2 });
        assertThat(key1.compareTo(key2), greaterThan(0));
    }

    public void testCompareCompositeKeyBiggerValue() {
        InternalComposite.ArrayMap key1 = createMap(Arrays.asList("field1", "field2"), new Comparable<?>[] { 1, 2 });
        InternalComposite.ArrayMap key2 = createMap(Arrays.asList("field3", "field2"), new Comparable<?>[] { 2, 3 });
        assertThat(key1.compareTo(key2), lessThan(0));
    }

    public void testCompareCompositeKeySmallerValue() {
        InternalComposite.ArrayMap key1 = createMap(Arrays.asList("field3", "field2"), new Comparable<?>[] { 1, 2 });
        InternalComposite.ArrayMap key2 = createMap(Arrays.asList("field1", "field2"), new Comparable<?>[] { 2, 3 });
        assertThat(key1.compareTo(key2), greaterThan(0));
    }

    public void testCompareCompositeKeyNullValueIsSmaller1() {
        InternalComposite.ArrayMap key1 = createMap(Arrays.asList("field1", "field2"), new Comparable<?>[] { null, 20 });
        InternalComposite.ArrayMap key2 = createMap(Arrays.asList("field1", "field2"), new Comparable<?>[] { 1, 2 });
        assertThat(key1.compareTo(key2), lessThan(0));
    }

    public void testCompareCompositeKeyNullValueIsSmaller2() {
        InternalComposite.ArrayMap key1 = createMap(Arrays.asList("field1", "field2"), new Comparable<?>[] { 1, 2 });
        InternalComposite.ArrayMap key2 = createMap(Arrays.asList("field1", "field2"), new Comparable<?>[] { null, 20 });
        assertThat(key1.compareTo(key2), greaterThan(0));
    }

    public void testCompareCompositeKeyMoreFieldsIsGreater() {
        InternalComposite.ArrayMap key1 = createMap(Arrays.asList("field1", "field2"), new Comparable<?>[] { 1, 2 });
        InternalComposite.ArrayMap key2 = createMap(Arrays.asList("field1", "field2", "field3"), new Comparable<?>[] { 1, 2, null });
        assertThat(key1.compareTo(key2), lessThan(0));
    }

    public void testCompareCompositeKeyLessFieldsIsLesser() {
        InternalComposite.ArrayMap key1 = createMap(Arrays.asList("field1", "field2", "field3"), new Comparable<?>[] { 1, 2, null });
        InternalComposite.ArrayMap key2 = createMap(Arrays.asList("field1", "field2"), new Comparable<?>[] { 1, 2 });
        assertThat(key1.compareTo(key2), greaterThan(0));
    }

    public void testCompareCompositeKeyEqual() {
        InternalComposite.ArrayMap key1 = createMap(Arrays.asList("field1", "field2", "field3"), new Comparable<?>[] { null, 1, 2 });
        InternalComposite.ArrayMap key2 = createMap(Arrays.asList("field1", "field2", "field3"), new Comparable<?>[] { null, 1, 2 });
        assertThat(key1.compareTo(key1), equalTo(0));
        assertThat(key1.equals(key1), is(true));

        assertThat(key1.compareTo(key2), equalTo(0));
        assertThat(key1.equals(key2), is(true));
        assertThat(key2.equals(key1), is(true));
    }

    public void testCompareCompositeKeyValuesHaveDifferentTypes() {
        InternalComposite.ArrayMap key1 = createMap(Arrays.asList("field1", "field2"), new Comparable<?>[] { 1, 2 });

        InternalComposite.ArrayMap key2 = createMap(Arrays.asList("field1", "field2"), new Comparable<?>[] { "1", 2 });

        ClassCastException exception = expectThrows(ClassCastException.class, () -> key1.compareTo(key2));
        assertThat(exception.getMessage(), containsString("java.lang.String cannot be cast to"));
    }

    public void testFormatObjectChecked() {
        DocValueFormat weekYearMonth = new DocValueFormat.DateTime(
            // YYYY is week-based year. The parser will ignore MM (month-of-year) and dd (day-of-month)
            DateFormatter.forPattern("YYYY-MM-dd"),
            ZoneOffset.UTC,
            DateFieldMapper.Resolution.MILLISECONDS
        );
        long epoch = 1622060077L; // May 25th 2021, evening
        expectThrows(IllegalArgumentException.class, () -> InternalComposite.formatObject(epoch, weekYearMonth));
    }

    public void testFormatDateEpochTimezone() {
        DocValueFormat epochSecond = new DocValueFormat.DateTime(
            // YYYY is week-based year. The parser will ignore MM (month-of-year) and dd (day-of-month)
            DateFormatter.forPattern("epoch_second"),
            ZoneOffset.ofHours(-2),
            DateFieldMapper.Resolution.MILLISECONDS
        );
        DocValueFormat epochMillis = new DocValueFormat.DateTime(
            // YYYY is week-based year. The parser will ignore MM (month-of-year) and dd (day-of-month)
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
        List<DocValueFormat> formats = IntStream.range(0, fields.size()).mapToObj(i -> DocValueFormat.RAW).toList();
        return new InternalComposite.ArrayMap(fields, formats, values);
    }
}
