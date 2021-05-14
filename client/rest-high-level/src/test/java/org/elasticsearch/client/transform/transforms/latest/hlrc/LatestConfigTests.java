/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms.latest.hlrc;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.transform.transforms.latest.LatestConfig;

import java.io.IOException;
import java.util.ArrayList;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class LatestConfigTests
        extends AbstractResponseTestCase<LatestConfig, org.elasticsearch.client.transform.transforms.latest.LatestConfig> {

    public static LatestConfig randomLatestConfig() {
        return new LatestConfig(
            new ArrayList<>(randomUnique(() -> randomAlphaOfLengthBetween(1, 10), randomIntBetween(1, 10))),
            randomAlphaOfLengthBetween(1, 10)
        );
    }

    @Override
    protected LatestConfig createServerTestInstance(XContentType xContentType) {
        return randomLatestConfig();
    }

    @Override
    protected org.elasticsearch.client.transform.transforms.latest.LatestConfig doParseToClientInstance(XContentParser parser)
        throws IOException {
        return org.elasticsearch.client.transform.transforms.latest.LatestConfig.fromXContent(parser);
    }

    @Override
    protected void assertInstances(
        LatestConfig serverTestInstance,
        org.elasticsearch.client.transform.transforms.latest.LatestConfig clientInstance
    ) {
        assertThat(serverTestInstance.getUniqueKey(), is(equalTo(clientInstance.getUniqueKey())));
        assertThat(serverTestInstance.getSort(), is(equalTo(clientInstance.getSort())));
    }
}
