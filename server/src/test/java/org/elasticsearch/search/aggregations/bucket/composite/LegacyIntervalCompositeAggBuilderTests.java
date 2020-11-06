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

package org.elasticsearch.search.aggregations.bucket.composite;

import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Duplicates the tests from {@link CompositeAggregationBuilderTests}, except using the deprecated
 * interval on date histo.  Separated to make testing the warnings easier.
 *
 * Can be removed in when the legacy interval options are gone
 */
public class LegacyIntervalCompositeAggBuilderTests extends BaseAggregationTestCase<CompositeAggregationBuilder> {

    private DateHistogramValuesSourceBuilder randomDateHistogramSourceBuilder() {
        DateHistogramValuesSourceBuilder histo = new DateHistogramValuesSourceBuilder(randomAlphaOfLengthBetween(5, 10));
        if (randomBoolean()) {
            histo.field(randomAlphaOfLengthBetween(1, 20));
        } else {
            histo.script(new Script(randomAlphaOfLengthBetween(10, 20)));
        }
        if (randomBoolean()) {
            histo.dateHistogramInterval(randomFrom(DateHistogramInterval.days(1),
                DateHistogramInterval.minutes(1), DateHistogramInterval.weeks(1)));
        } else {
            histo.interval(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            histo.timeZone(randomZone());
        }
        if (randomBoolean()) {
            histo.missingBucket(true);
        }
        return histo;
    }

    private TermsValuesSourceBuilder randomTermsSourceBuilder() {
        TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder(randomAlphaOfLengthBetween(5, 10));
        if (randomBoolean()) {
            terms.field(randomAlphaOfLengthBetween(1, 20));
        } else {
            terms.script(new Script(randomAlphaOfLengthBetween(10, 20)));
        }
        terms.order(randomFrom(SortOrder.values()));
        if (randomBoolean()) {
            terms.missingBucket(true);
        }
        return terms;
    }

    private HistogramValuesSourceBuilder randomHistogramSourceBuilder() {
        HistogramValuesSourceBuilder histo = new HistogramValuesSourceBuilder(randomAlphaOfLengthBetween(5, 10));
        if (randomBoolean()) {
            histo.field(randomAlphaOfLengthBetween(1, 20));
        } else {
            histo.script(new Script(randomAlphaOfLengthBetween(10, 20)));
        }
        if (randomBoolean()) {
            histo.missingBucket(true);
        }
        histo.interval(randomDoubleBetween(Math.nextUp(0), Double.MAX_VALUE, false));
        return histo;
    }

    @Override
    protected CompositeAggregationBuilder createTestAggregatorBuilder() {
        int numSources = randomIntBetween(1, 10);
        List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>();
        // ensure we add at least one date histo
        sources.add(randomDateHistogramSourceBuilder());
        for (int i = 0; i < numSources; i++) {
            int type = randomIntBetween(0, 2);
            switch (type) {
                case 0:
                    sources.add(randomTermsSourceBuilder());
                    break;
                case 1:
                    sources.add(randomDateHistogramSourceBuilder());
                    break;
                case 2:
                    sources.add(randomHistogramSourceBuilder());
                    break;
                default:
                    throw new AssertionError("wrong branch");
            }
        }
        return new CompositeAggregationBuilder(randomAlphaOfLength(10), sources);
    }

    @Override
    public void testFromXContent() throws IOException {
        super.testFromXContent();
        assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
    }

    @Override
    public void testFromXContentMulti() throws IOException {
        super.testFromXContentMulti();
        assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
    }

    @Override
    public void testSerializationMulti() throws IOException {
        super.testSerializationMulti();
        assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
    }

    @Override
    public void testToString() throws IOException {
        super.testToString();
        assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
    }

    @Override
    public void testSerialization() throws IOException {
        super.testSerialization();
        assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
    }

    @Override
    public void testEqualsAndHashcode() throws IOException {
        super.testEqualsAndHashcode();
        assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
    }

    @Override
    public void testShallowCopy() {
        super.testShallowCopy();
        assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
    }
}
