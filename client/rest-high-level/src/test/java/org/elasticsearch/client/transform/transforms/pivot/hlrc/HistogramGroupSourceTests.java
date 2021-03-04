/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
