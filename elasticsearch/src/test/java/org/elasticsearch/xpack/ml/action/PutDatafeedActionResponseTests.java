/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.xpack.ml.action.PutDatafeedAction.Response;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfigTests;
import org.elasticsearch.xpack.ml.support.AbstractStreamableTestCase;

import java.util.Arrays;

public class PutDatafeedActionResponseTests extends AbstractStreamableTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        DatafeedConfig.Builder datafeedConfig = new DatafeedConfig.Builder(
                DatafeedConfigTests.randomValidDatafeedId(), randomAsciiOfLength(10));
        datafeedConfig.setIndexes(Arrays.asList(randomAsciiOfLength(10)));
        datafeedConfig.setTypes(Arrays.asList(randomAsciiOfLength(10)));
        return new Response(randomBoolean(), datafeedConfig.build());
    }

    @Override
    protected Response createBlankInstance() {
        return new Response();
    }

}
