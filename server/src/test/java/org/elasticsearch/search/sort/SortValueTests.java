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
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.test.AbstractNamedWriteableTestCase;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.time.ZoneId;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class SortValueTests extends AbstractNamedWriteableTestCase<SortValue> {
    private static final DocValueFormat STRICT_DATE_TIME = new DocValueFormat.DateTime(DateFormatter.forPattern("strict_date_time"),
            ZoneId.of("UTC"), DateFieldMapper.Resolution.MILLISECONDS);

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
        switch (between(0, 2)) {
            case 0: return SortValue.from(randomDouble());
            case 1: return SortValue.from(randomLong());
            case 2: return SortValue.from(new BytesRef(randomAlphaOfLength(5)));
            default: throw new AssertionError();
        }
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
