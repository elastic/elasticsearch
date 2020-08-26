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

package org.elasticsearch.client.transform.transforms.pivot;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.function.Predicate;

public class HistogramGroupSourceTests extends AbstractXContentTestCase<HistogramGroupSource> {

    public static HistogramGroupSource randomHistogramGroupSource() {
        String field = randomAlphaOfLengthBetween(1, 20);
        Script script = randomBoolean() ? new Script(randomAlphaOfLengthBetween(1, 10)) : null;
        boolean missingBucket = randomBoolean();
        double interval = randomDoubleBetween(Math.nextUp(0), Double.MAX_VALUE, false);
        return new HistogramGroupSource(field, script, missingBucket, interval);
    }

    @Override
    protected HistogramGroupSource doParseInstance(XContentParser parser) throws IOException {
        return HistogramGroupSource.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected HistogramGroupSource createTestInstance() {
        return randomHistogramGroupSource();
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // allow unknown fields in the root of the object only
        return field -> !field.isEmpty();
    }
}
