/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.sort;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.test.AbstractNamedWriteableTestCase;

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
        return randomBoolean() ? SortValue.forDouble(randomDouble()) : SortValue.forLong(randomLong());
    }

    @Override
    protected SortValue mutateInstance(SortValue instance) throws IOException {
        return randomValueOtherThanMany(c -> c.getKey().equals(instance.getKey()), this::createTestInstance);
    }

    public void testFormatDouble() {
        assertThat(SortValue.forDouble(1.0).format(DocValueFormat.RAW), equalTo("1.0"));
        // The date formatter coerces the double into a long to format it
        assertThat(SortValue.forDouble(1.0).format(STRICT_DATE_TIME), equalTo("1970-01-01T00:00:00.001Z"));
    }

    public void testFormatLong() {
        assertThat(SortValue.forLong(1).format(DocValueFormat.RAW), equalTo("1"));
        assertThat(SortValue.forLong(1).format(STRICT_DATE_TIME), equalTo("1970-01-01T00:00:00.001Z"));
    }

    public void testToXContentDouble() {
        assertThat(toXContent(SortValue.forDouble(1.0), DocValueFormat.RAW), equalTo("{\"test\":1.0}"));
        assertThat(toXContent(SortValue.forDouble(1.0), STRICT_DATE_TIME), equalTo("{\"test\":\"1970-01-01T00:00:00.001Z\"}"));
    }

    public void testToXContentLong() {
        assertThat(toXContent(SortValue.forLong(1), DocValueFormat.RAW), equalTo("{\"test\":1}"));
        assertThat(toXContent(SortValue.forLong(1), STRICT_DATE_TIME), equalTo("{\"test\":\"1970-01-01T00:00:00.001Z\"}"));
    }

    public void testCompareDifferentTypes() {
        assertThat(SortValue.forDouble(1.0), lessThan(SortValue.forLong(1)));
        assertThat(SortValue.forDouble(Double.MAX_VALUE), lessThan(SortValue.forLong(Long.MIN_VALUE)));
        assertThat(SortValue.forLong(1), greaterThan(SortValue.forDouble(1.0)));
        assertThat(SortValue.forLong(Long.MIN_VALUE), greaterThan(SortValue.forDouble(Double.MAX_VALUE)));
    }

    public void testCompareDoubles() {
        double r = randomDouble();
        assertThat(SortValue.forDouble(r), equalTo(SortValue.forDouble(r)));
        assertThat(SortValue.forDouble(r), lessThan(SortValue.forDouble(r + 1)));
        assertThat(SortValue.forDouble(r), greaterThan(SortValue.forDouble(r - 1)));
    }

    public void testCompareLongs() {
        long r = randomLongBetween(Long.MIN_VALUE + 1, Long.MAX_VALUE - 1);
        assertThat(SortValue.forLong(r), equalTo(SortValue.forLong(r)));
        assertThat(SortValue.forLong(r), lessThan(SortValue.forLong(r + 1)));
        assertThat(SortValue.forLong(r), greaterThan(SortValue.forLong(r - 1)));
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
