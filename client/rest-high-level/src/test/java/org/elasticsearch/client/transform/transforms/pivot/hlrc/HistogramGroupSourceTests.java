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
import org.elasticsearch.xpack.core.transform.transforms.pivot.HistogramGroupSource;
import org.elasticsearch.xpack.core.transform.transforms.pivot.ScriptConfig;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class HistogramGroupSourceTests extends AbstractResponseTestCase<
    HistogramGroupSource,
    org.elasticsearch.client.transform.transforms.pivot.HistogramGroupSource> {

    public static HistogramGroupSource randomHistogramGroupSource() {
        String field = randomBoolean() ? null : randomAlphaOfLengthBetween(1, 20);
        ScriptConfig scriptConfig = randomBoolean() ? null : DateHistogramGroupSourceTests.randomScriptConfig();
        boolean missingBucket = randomBoolean();
        double interval = randomDoubleBetween(Math.nextUp(0), Double.MAX_VALUE, false);
        return new HistogramGroupSource(field, scriptConfig, missingBucket, interval);
    }

    @Override
    protected HistogramGroupSource createServerTestInstance(XContentType xContentType) {
        return randomHistogramGroupSource();
    }

    @Override
    protected org.elasticsearch.client.transform.transforms.pivot.HistogramGroupSource doParseToClientInstance(XContentParser parser)
        throws IOException {
        return org.elasticsearch.client.transform.transforms.pivot.HistogramGroupSource.fromXContent(parser);
    }

    @Override
    protected void assertInstances(
        HistogramGroupSource serverTestInstance,
        org.elasticsearch.client.transform.transforms.pivot.HistogramGroupSource clientInstance
    ) {
        assertThat(serverTestInstance.getField(), equalTo(clientInstance.getField()));
        if (serverTestInstance.getScriptConfig() != null) {
            assertThat(serverTestInstance.getScriptConfig().getScript(), equalTo(clientInstance.getScript()));
        } else {
            assertNull(clientInstance.getScript());
        }
        assertThat(serverTestInstance.getInterval(), equalTo(clientInstance.getInterval()));
    }

}
