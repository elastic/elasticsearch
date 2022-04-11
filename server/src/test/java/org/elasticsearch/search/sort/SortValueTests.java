/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.sort;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.test.AbstractNamedWriteableTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class SortValueTests extends AbstractNamedWriteableTestCase<SortValue> {
    private static final DocValueFormat STRICT_DATE_TIME = new DocValueFormat.DateTime(
        DateFormatter.forPattern("strict_date_time"),
        ZoneId.of("UTC"),
        DateFieldMapper.Resolution.MILLISECONDS
    );

    @Override
    protected Class<SortValue> categoryClass() {
        return SortValue.class;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(SortValue.namedWriteables());
    }

    @Override
    protected SortValue createTestInstance() {
        return switch (between(0, 3)) {
            case 0 -> SortValue.from(randomDouble());
            case 1 -> SortValue.from(randomLong());
            case 2 -> SortValue.from(new BytesRef(randomAlphaOfLength(5)));
            case 3 -> SortValue.empty();
            default -> throw new AssertionError();
        };
    }

    @Override
    protected SortValue mutateInstance(SortValue instance) throws IOException {
        return randomValueOtherThanMany(mut -> instance.getKey().equals(mut.getKey()), this::createTestInstance);
    }

    public void testFormatDouble() {
        assertThat(SortValue.from(1.0).format(DocValueFormat.RAW), equalTo("1.0"));
        // The date formatter coerces the double into a long to format it
        assertThat(SortValue.from(1.0).format(STRICT_DATE_TIME), equalTo("1970-01-01T00:00:00.001Z"));
    }

    public void testFormatLong() {
        assertThat(SortValue.from(1).format(DocValueFormat.RAW), equalTo("1"));
        assertThat(SortValue.from(1).format(STRICT_DATE_TIME), equalTo("1970-01-01T00:00:00.001Z"));
    }

    public void testFormatEmpty() {
        assertThat(SortValue.empty().format(DocValueFormat.RAW), equalTo(""));
    }

    public void testToXContentDouble() {
        assertThat(toXContent(SortValue.from(1.0), DocValueFormat.RAW), equalTo("{\"test\":1.0}"));
        // The date formatter coerces the double into a long to format it
        assertThat(toXContent(SortValue.from(1.0), STRICT_DATE_TIME), equalTo("{\"test\":\"1970-01-01T00:00:00.001Z\"}"));
    }

    public void testToXContentLong() {
        assertThat(toXContent(SortValue.from(1), DocValueFormat.RAW), equalTo("{\"test\":1}"));
        assertThat(toXContent(SortValue.from(1), STRICT_DATE_TIME), equalTo("{\"test\":\"1970-01-01T00:00:00.001Z\"}"));
    }

    public void testToXContentBytes() {
        assertThat(toXContent(SortValue.from(new BytesRef("cat")), DocValueFormat.RAW), equalTo("{\"test\":\"cat\"}"));
        assertThat(
            toXContent(SortValue.from(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("127.0.0.1")))), DocValueFormat.IP),
            equalTo("{\"test\":\"127.0.0.1\"}")
        );
    }

    public void testToXContentEmpty() {
        assertThat(toXContent(SortValue.empty(), DocValueFormat.RAW), equalTo("{\"test\"}"));
    }

    public void testCompareDifferentTypes() {
        assertThat(SortValue.from(1.0), lessThan(SortValue.from(1)));
        assertThat(SortValue.from(Double.MAX_VALUE), lessThan(SortValue.from(Long.MIN_VALUE)));
        assertThat(SortValue.from(1), greaterThan(SortValue.from(1.0)));
        assertThat(SortValue.from(Long.MIN_VALUE), greaterThan(SortValue.from(Double.MAX_VALUE)));
        assertThat(SortValue.from(new BytesRef("cat")), lessThan(SortValue.from(1)));
        assertThat(SortValue.from(1), greaterThan(SortValue.from(new BytesRef("cat"))));
        assertThat(SortValue.from(new BytesRef("cat")), lessThan(SortValue.from(1.0)));
        assertThat(SortValue.from(1.0), greaterThan(SortValue.from(new BytesRef("cat"))));
    }

    /**
     * When comparing different types ordering takes place according to the writable name.
     * This is the reason why "long" is greater than "empty" and "double" is less than "empty".
     * See {@link org.elasticsearch.search.sort.SortValue#compareTo}.
     */
    public void testCompareToEmpty() {
        assertThat(SortValue.from(1.0), lessThan(SortValue.empty()));
        assertThat(SortValue.from(Double.MAX_VALUE), lessThan(SortValue.empty()));
        assertThat(SortValue.from(Double.NaN), equalTo(SortValue.empty()));
        assertThat(SortValue.from(1), lessThan(SortValue.empty()));
        assertThat(SortValue.from(Long.MIN_VALUE), lessThan(SortValue.empty()));
        assertThat(SortValue.from(new BytesRef("cat")), lessThan(SortValue.empty()));
    }

    public void testCompareDoubles() {
        double r = randomDouble();
        assertThat(SortValue.from(r), equalTo(SortValue.from(r)));
        assertThat(SortValue.from(r), lessThan(SortValue.from(r + 1)));
        assertThat(SortValue.from(r), greaterThan(SortValue.from(r - 1)));
    }

    public void testCompareLongs() {
        long r = randomLongBetween(Long.MIN_VALUE + 1, Long.MAX_VALUE - 1);
        assertThat(SortValue.from(r), equalTo(SortValue.from(r)));
        assertThat(SortValue.from(r), lessThan(SortValue.from(r + 1)));
        assertThat(SortValue.from(r), greaterThan(SortValue.from(r - 1)));
    }

    public void testCompareEmpty() {
        assertThat(SortValue.empty(), equalTo(SortValue.empty()));
    }

    public void testSortValueOrdering() {
        final SortValue maxLong = SortValue.from(Long.MAX_VALUE);
        final SortValue minLong = SortValue.from(Long.MIN_VALUE);
        final SortValue negativeLong = SortValue.from(-12L);
        final SortValue zeroLong = SortValue.from(0L);
        final SortValue positiveLong = SortValue.from(110L);
        final SortValue negativeNan = SortValue.from(-Double.NaN);
        final SortValue positiveNan = SortValue.from(Double.NaN);
        final SortValue maxDouble = SortValue.from(Double.MAX_VALUE);
        final SortValue minDouble = SortValue.from(Double.MIN_VALUE);
        final SortValue negativeDouble = SortValue.from(-30.5D);
        final SortValue zeroDouble = SortValue.from(0.0D);
        final SortValue positiveDouble = SortValue.from(18.97D);
        final SortValue emptyBytesRef = SortValue.from(new BytesRef(""));
        final SortValue fooBytesRef = SortValue.from(new BytesRef("Foo"));
        final SortValue barBytesRef = SortValue.from(new BytesRef("bar"));
        final SortValue valueless = SortValue.empty();
        final List<SortValue> values = List.of(
            maxLong,
            minLong,
            negativeLong,
            zeroLong,
            positiveLong,
            negativeNan,
            positiveNan,
            maxDouble,
            minDouble,
            negativeDouble,
            zeroDouble,
            positiveDouble,
            emptyBytesRef,
            fooBytesRef,
            barBytesRef,
            valueless
        );

        final List<SortValue> sortedValues = values.stream().sorted(Comparator.naturalOrder()).collect(Collectors.toList());

        /**
         * `negativeNan` and `positiveNan` are instances of
         * {@link org.elasticsearch.search.sort.SortValue.ValuelessSortValue}
         * the same of {@link SortValue#empty()}.
         */
        assertThat(
            sortedValues,
            equalTo(
                List.of(
                    emptyBytesRef,
                    fooBytesRef,
                    barBytesRef,
                    negativeDouble,
                    zeroDouble,
                    minDouble,
                    positiveDouble,
                    maxDouble,
                    minLong,
                    negativeLong,
                    zeroLong,
                    positiveLong,
                    maxLong,
                    negativeNan,
                    positiveNan,
                    valueless
                )
            )
        );
    }

    public void testBytes() {
        String r = randomAlphaOfLength(5);
        assertThat(SortValue.from(new BytesRef(r)), equalTo(SortValue.from(new BytesRef(r))));
        assertThat(SortValue.from(new BytesRef(r)), lessThan(SortValue.from(new BytesRef(r + "with_suffix"))));
        assertThat(SortValue.from(new BytesRef(r)), greaterThan(SortValue.from(new BytesRef(new byte[] {}))));
    }

    public void testSerializeBytesToOldVersion() {
        SortValue value = SortValue.from(new BytesRef("can't send me!"));
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_7_0_0, Version.V_7_10_1);
        Exception e = expectThrows(IllegalArgumentException.class, () -> copyInstance(value, version));
        assertThat(
            e.getMessage(),
            equalTo(
                "versions of Elasticsearch before 7.11.0 can't handle non-numeric sort values and attempted to send to [" + version + "]"
            )
        );
    }

    public String toXContent(SortValue sortValue, DocValueFormat format) {
        return Strings.toString(new ToXContentFragment() {
            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.field("test");
                return sortValue.toXContent(builder, format);
            }
        });
    }
}
