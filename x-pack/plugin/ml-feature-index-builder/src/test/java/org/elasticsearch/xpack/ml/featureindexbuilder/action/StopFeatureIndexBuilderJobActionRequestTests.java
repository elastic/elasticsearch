/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.featureindexbuilder.action;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.xpack.ml.featureindexbuilder.action.StopFeatureIndexBuilderJobAction.Request;
import java.io.IOException;

public class StopFeatureIndexBuilderJobActionRequestTests extends AbstractStreamableXContentTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        return new Request(randomAlphaOfLengthBetween(1, 10));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected Request doParseInstance(XContentParser parser) throws IOException {
        return Request.PARSER.parse(parser, null);
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }
}
