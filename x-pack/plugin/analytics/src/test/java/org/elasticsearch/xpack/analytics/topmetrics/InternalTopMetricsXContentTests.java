/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.topmetrics;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.equalTo;

public class InternalTopMetricsXContentTests extends ESTestCase {
    public void testDoubleSortValue() throws IOException {
        XContentBuilder b = JsonXContent.contentBuilder().prettyPrint();
        b.startObject();
        metrics(DocValueFormat.RAW, 1.0, "test", 1.0).toXContent(b, ToXContent.EMPTY_PARAMS);
        b.endObject();
        assertThat(Strings.toString(b), equalTo(
                "{\n" +
                "  \"test\" : {\n" +
                "    \"top\" : [\n" +
                "      {\n" +
                "        \"sort\" : [\n" +
                "          1.0\n" +
                "        ],\n" +
                "        \"metrics\" : {\n" +
                "          \"test\" : 1.0\n" +
                "        }\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}"));
    }

    public void testDateSortValue() throws IOException {
        DocValueFormat sortFormat = new DocValueFormat.DateTime(DateFormatter.forPattern("strict_date_time"), ZoneId.of("UTC"),
                DateFieldMapper.Resolution.MILLISECONDS);
        long sortValue = ZonedDateTime.parse("2007-12-03T10:15:30Z").toInstant().toEpochMilli();
        XContentBuilder b = JsonXContent.contentBuilder().prettyPrint();
        b.startObject();
        metrics(sortFormat, sortValue, "test", 1.0).toXContent(b, ToXContent.EMPTY_PARAMS);
        b.endObject();
        assertThat(Strings.toString(b), equalTo(
                "{\n" +
                "  \"test\" : {\n" +
                "    \"top\" : [\n" +
                "      {\n" +
                "        \"sort\" : [\n" +
                "          \"2007-12-03T10:15:30.000Z\"\n" +
                "        ],\n" +
                "        \"metrics\" : {\n" +
                "          \"test\" : 1.0\n" +
                "        }\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}"));
    }

    private InternalTopMetrics metrics(DocValueFormat sortFormat, Object sortValue, String metricName, double metricValue) {
        SortOrder sortOrder = randomFrom(SortOrder.values());
        return new InternalTopMetrics("test", sortFormat, sortOrder, sortValue, metricName, metricValue, emptyList(), null);
    }
}
