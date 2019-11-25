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

package org.elasticsearch.client.transform.transforms.pivot.hlrc;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.xpack.core.transform.transforms.pivot.DateHistogramGroupSource;

import static org.hamcrest.Matchers.equalTo;

public class DateHistogramGroupSourceTests extends AbstractResponseTestCase<
        DateHistogramGroupSource,
        org.elasticsearch.client.transform.transforms.pivot.DateHistogramGroupSource> {

    public static DateHistogramGroupSource randomDateHistogramGroupSource() {
        String field = randomAlphaOfLengthBetween(1, 20);
        DateHistogramGroupSource dateHistogramGroupSource; // = new DateHistogramGroupSource(field);
        if (randomBoolean()) {
            dateHistogramGroupSource = new DateHistogramGroupSource(field, new DateHistogramGroupSource.FixedInterval(
                    new DateHistogramInterval(randomPositiveTimeValue())));
        } else {
            dateHistogramGroupSource = new DateHistogramGroupSource(field, new DateHistogramGroupSource.CalendarInterval(
                    new DateHistogramInterval(randomTimeValue(1,1, "m", "h", "d", "w"))));
        }

        if (randomBoolean()) {
            dateHistogramGroupSource.setTimeZone(randomZone());
        }
        return dateHistogramGroupSource;
    }

    @Override
    protected DateHistogramGroupSource createServerTestInstance(XContentType xContentType) {
        return randomDateHistogramGroupSource();
    }

    @Override
    protected org.elasticsearch.client.transform.transforms.pivot.DateHistogramGroupSource doParseToClientInstance(XContentParser parser) {
        return org.elasticsearch.client.transform.transforms.pivot.DateHistogramGroupSource.fromXContent(parser);
    }

    @Override
    protected void assertInstances(DateHistogramGroupSource serverTestInstance,
            org.elasticsearch.client.transform.transforms.pivot.DateHistogramGroupSource clientInstance) {
        assertThat(serverTestInstance.getField(), equalTo(clientInstance.getField()));
        assertSameInterval(serverTestInstance.getInterval(), clientInstance.getInterval());
        assertThat(serverTestInstance.getTimeZone(), equalTo(clientInstance.getTimeZone()));
        assertThat(serverTestInstance.getType().name(), equalTo(clientInstance.getType().name()));
    }

    private void assertSameInterval(DateHistogramGroupSource.Interval serverTestInstance,
            org.elasticsearch.client.transform.transforms.pivot.DateHistogramGroupSource.Interval clientInstance) {
        assertEquals(serverTestInstance.getName(), clientInstance.getName());
        assertEquals(serverTestInstance.getInterval(), clientInstance.getInterval());
    }

}
