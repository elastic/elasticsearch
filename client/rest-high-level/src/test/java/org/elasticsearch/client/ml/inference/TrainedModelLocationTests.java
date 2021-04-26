/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml.inference;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class TrainedModelLocationTests extends AbstractXContentTestCase<TrainedModelLocation> {

    static TrainedModelLocation randomInstance() {
        return new TrainedModelLocation(randomAlphaOfLength(7), randomAlphaOfLength(7));
    }

    @Override
    protected TrainedModelLocation createTestInstance() {
        return randomInstance();
    }

    @Override
    protected TrainedModelLocation doParseInstance(XContentParser parser) throws IOException {
        return TrainedModelLocation.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
