/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms.pivot;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class DateHistogramGroupSourceTests extends AbstractSerializingTestCase<DateHistogramGroupSource> {

    public static DateHistogramGroupSource randomDateHistogramGroupSource() {
        String field = randomAlphaOfLengthBetween(1, 20);
        DateHistogramGroupSource dateHistogramGroupSource;
        if (randomBoolean()) {
            dateHistogramGroupSource = new DateHistogramGroupSource(field, new DateHistogramGroupSource.FixedInterval(
                    new DateHistogramInterval(randomPositiveTimeValue())));
        } else {
            dateHistogramGroupSource = new DateHistogramGroupSource(field, new DateHistogramGroupSource.CalendarInterval(
                    new DateHistogramInterval(randomTimeValue(1, 1, "m", "h", "d", "w"))));
        }

        if (randomBoolean()) {
            dateHistogramGroupSource.setTimeZone(randomZone());
        }
        return dateHistogramGroupSource;
    }

    public void testBackwardsSerialization() throws IOException {
        DateHistogramGroupSource groupSource = randomDateHistogramGroupSource();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(Version.V_7_2_0);
            groupSource.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                in.setVersion(Version.V_7_2_0);
                DateHistogramGroupSource streamedGroupSource = new DateHistogramGroupSource(in);
                assertEquals(groupSource, streamedGroupSource);
            }
        }
    }

    @Override
    protected DateHistogramGroupSource doParseInstance(XContentParser parser) throws IOException {
        return DateHistogramGroupSource.fromXContent(parser, false);
    }

    @Override
    protected DateHistogramGroupSource createTestInstance() {
        return randomDateHistogramGroupSource();
    }

    @Override
    protected Reader<DateHistogramGroupSource> instanceReader() {
        return DateHistogramGroupSource::new;
    }

}
