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
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class ExponentialHistogramXContentTests extends ExponentialHistogramTestCase {

    public void testEmptyHistogram() {
        ExponentialHistogram emptyHistogram = ExponentialHistogram.empty();
        assertThat(toJson(emptyHistogram), equalTo("{\"scale\":" + emptyHistogram.scale() + "}"));
    }

    public void testFullHistogram() {
        FixedCapacityExponentialHistogram histo = createAutoReleasedHistogram(100);
        histo.setZeroBucket(new ZeroBucket(0.1234, 42));
        histo.resetBuckets(7);
        histo.tryAddBucket(-10, 15, false);
        histo.tryAddBucket(10, 5, false);
        histo.tryAddBucket(-11, 10, true);
        histo.tryAddBucket(11, 20, true);
        assertThat(
            toJson(histo),
            equalTo(
                "{"
                    + "\"scale\":7,"
                    + "\"zero\":{\"count\":42,\"threshold\":0.1234},"
                    + "\"positive\":{\"indices\":[-11,11],\"counts\":[10,20]},"
                    + "\"negative\":{\"indices\":[-10,10],\"counts\":[15,5]}"
                    + "}"
            )
        );
    }

    public void testOnlyZeroThreshold() {
        FixedCapacityExponentialHistogram histo = createAutoReleasedHistogram(10);
        histo.setZeroBucket(new ZeroBucket(5.0, 0));
        histo.resetBuckets(3);
        assertThat(toJson(histo), equalTo("{\"scale\":3,\"zero\":{\"threshold\":5.0}}"));
    }

    public void testOnlyZeroCount() {
        FixedCapacityExponentialHistogram histo = createAutoReleasedHistogram(10);
        histo.setZeroBucket(new ZeroBucket(0.0, 7));
        histo.resetBuckets(2);
        assertThat(toJson(histo), equalTo("{\"scale\":2,\"zero\":{\"count\":7}}"));
    }

    public void testOnlyPositiveBuckets() {
        FixedCapacityExponentialHistogram histo = createAutoReleasedHistogram(10);
        histo.resetBuckets(4);
        histo.tryAddBucket(-1, 3, true);
        histo.tryAddBucket(2, 5, true);
        assertThat(toJson(histo), equalTo("{\"scale\":4,\"positive\":{\"indices\":[-1,2],\"counts\":[3,5]}}"));
    }

    public void testOnlyNegativeBuckets() {
        FixedCapacityExponentialHistogram histo = createAutoReleasedHistogram(10);
        histo.resetBuckets(5);
        histo.tryAddBucket(-1, 4, false);
        histo.tryAddBucket(2, 6, false);
        assertThat(toJson(histo), equalTo("{\"scale\":5,\"negative\":{\"indices\":[-1,2],\"counts\":[4,6]}}"));
    }

    private static String toJson(ExponentialHistogram histo) {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            ExponentialHistogramXContent.serialize(builder, histo);
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
