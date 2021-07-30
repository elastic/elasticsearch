/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.datafeed.DatafeedConfig;
import org.elasticsearch.client.ml.datafeed.DatafeedConfigTests;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;


public class PutDatafeedRequestTests extends AbstractXContentTestCase<PutDatafeedRequest> {

    @Override
    protected PutDatafeedRequest createTestInstance() {
        return new PutDatafeedRequest(DatafeedConfigTests.createRandom());
    }

    @Override
    protected PutDatafeedRequest doParseInstance(XContentParser parser) {
        return new PutDatafeedRequest(DatafeedConfig.PARSER.apply(parser, null).build());
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
