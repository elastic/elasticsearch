/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.dataframe.transforms.pivot;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class DateHistogramGroupSourceTests extends AbstractXContentTestCase<DateHistogramGroupSource> {

    public static DateHistogramGroupSource.Interval randomDateHistogramInterval() {
        if (randomBoolean()) {
            return new DateHistogramGroupSource.FixedInterval(new DateHistogramInterval(randomPositiveTimeValue()));
        } else {
            return new DateHistogramGroupSource.CalendarInterval(new DateHistogramInterval(randomTimeValue(1, 1, "m", "h", "d", "w")));
        }
    }

    public static DateHistogramGroupSource randomDateHistogramGroupSource() {
        String field = randomAlphaOfLengthBetween(1, 20);
        return new DateHistogramGroupSource(field,
                randomDateHistogramInterval(),
                randomBoolean() ? randomZone() : null);
    }

    @Override
    protected DateHistogramGroupSource createTestInstance() {
        return randomDateHistogramGroupSource();
    }

    @Override
    protected DateHistogramGroupSource doParseInstance(XContentParser parser) throws IOException {
        return DateHistogramGroupSource.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
