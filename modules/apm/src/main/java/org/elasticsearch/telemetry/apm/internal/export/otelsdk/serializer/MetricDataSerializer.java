/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk.serializer;

import io.opentelemetry.sdk.metrics.data.MetricData;

import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Serializes and deserializes OTel SDK {@link MetricData} batches using SMILE-encoded XContent.
 */
public final class MetricDataSerializer {

    private MetricDataSerializer() {}

    public static void serialize(Collection<MetricData> metrics, OutputStream out) throws IOException {
        try (XContentBuilder builder = XContentFactory.smileBuilder(out)) {
            builder.startArray();
            for (MetricData m : metrics) {
                MetricRecord record = MetricRecord.fromMetricData(m);
                if (record != null) {
                    builder.value(record);
                }
            }
            builder.endArray();
        }
    }

    public static List<MetricData> deserialize(InputStream in) throws IOException {
        try (XContentParser parser = XContentType.SMILE.xContent().createParser(XContentParserConfiguration.EMPTY, in)) {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.nextToken(), parser);
            List<MetricData> result = new ArrayList<>();
            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                MetricData m = MetricRecord.PARSER.parse(parser, null).toMetricData();
                if (m != null) {
                    result.add(m);
                }
            }
            return result;
        }
    }
}
