/*
 * Copyright Elasticsearch B.V., and/or licensed to Elasticsearch B.V.
 * under one or more license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
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
 *
 * This file is based on a modification of https://github.com/open-telemetry/opentelemetry-java which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.exponentialhistogram;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class ExponentialHistogramXContentTests extends ExponentialHistogramTestCase {

    public void testNullHistogram() {
        assertThat(toJson(null), equalTo("null"));
        checkRoundTrip(null);
    }

    public void testEmptyHistogram() {
        ExponentialHistogram emptyHistogram = ExponentialHistogram.empty();
        assertThat(toJson(emptyHistogram), equalTo("{\"scale\":" + emptyHistogram.scale() + "}"));
    }

    public void testFullHistogram() {
        ExponentialHistogram histo = createAutoReleasedHistogram(
            b -> b.zeroBucket(ZeroBucket.create(0.1234, 42))
                .scale(7)
                .sum(1234.56)
                .min(-321.123)
                .max(123.123)
                .setNegativeBucket(-10, 15)
                .setNegativeBucket(10, 5)
                .setPositiveBucket(-11, 10)
                .setPositiveBucket(11, 20)
        );
        assertThat(
            toJson(histo),
            equalTo(
                "{"
                    + "\"scale\":7,"
                    + "\"sum\":1234.56,"
                    + "\"min\":-321.123,"
                    + "\"max\":123.123,"
                    + "\"zero\":{\"count\":42,\"threshold\":0.1234},"
                    + "\"positive\":{\"indices\":[-11,11],\"counts\":[10,20]},"
                    + "\"negative\":{\"indices\":[-10,10],\"counts\":[15,5]}"
                    + "}"
            )
        );
        checkRoundTrip(histo);
    }

    public void testOnlyZeroThreshold() {
        ExponentialHistogram histo = createAutoReleasedHistogram(b -> b.scale(3).sum(1.1).zeroBucket(ZeroBucket.create(5.0, 0)));
        assertThat(toJson(histo), equalTo("{\"scale\":3,\"sum\":1.1,\"zero\":{\"threshold\":5.0}}"));
        checkRoundTrip(histo);
    }

    public void testOnlyZeroCount() {
        ExponentialHistogram histo = createAutoReleasedHistogram(
            b -> b.zeroBucket(ZeroBucket.create(0.0, 7)).scale(2).sum(1.1).min(0).max(0)
        );
        assertThat(toJson(histo), equalTo("{\"scale\":2,\"sum\":1.1,\"min\":0.0,\"max\":0.0,\"zero\":{\"count\":7}}"));
        checkRoundTrip(histo);
    }

    public void testOnlyPositiveBuckets() {
        ExponentialHistogram histo = createAutoReleasedHistogram(
            b -> b.scale(4).sum(1.1).min(0.5).max(2.5).setPositiveBucket(-1, 3).setPositiveBucket(2, 5)
        );
        assertThat(
            toJson(histo),
            equalTo("{\"scale\":4,\"sum\":1.1,\"min\":0.5,\"max\":2.5,\"positive\":{\"indices\":[-1,2],\"counts\":[3,5]}}")
        );
        checkRoundTrip(histo);
    }

    public void testOnlyNegativeBuckets() {
        ExponentialHistogram histo = createAutoReleasedHistogram(
            b -> b.scale(5).sum(1.1).min(-0.5).max(-0.25).setNegativeBucket(-1, 4).setNegativeBucket(2, 6)
        );
        assertThat(
            toJson(histo),
            equalTo("{\"scale\":5,\"sum\":1.1,\"min\":-0.5,\"max\":-0.25,\"negative\":{\"indices\":[-1,2],\"counts\":[4,6]}}")
        );
        checkRoundTrip(histo);
    }

    private static String toJson(ExponentialHistogram histo) {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            ExponentialHistogramXContent.serialize(builder, histo);
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void checkRoundTrip(ExponentialHistogram histo) {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            ExponentialHistogramXContent.serialize(builder, histo);
            String json = Strings.toString(builder);
            try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json)) {
                ExponentialHistogram parsed = ExponentialHistogramXContent.parseForTesting(parser);
                assertThat(parsed, equalTo(histo));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
