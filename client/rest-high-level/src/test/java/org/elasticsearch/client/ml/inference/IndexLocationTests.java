/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml.inference;

import org.elasticsearch.client.ml.inference.trainedmodel.IndexLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class IndexLocationTests extends AbstractXContentTestCase<IndexLocation> {

    static IndexLocation randomInstance() {
        return new IndexLocation(randomAlphaOfLength(7));
    }

    @Override
    protected IndexLocation createTestInstance() {
        return randomInstance();
    }

    @Override
    protected IndexLocation doParseInstance(XContentParser parser) throws IOException {
        return IndexLocation.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
