/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe.evaluation.common;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class AucRocPointTests extends AbstractXContentTestCase<AucRocPoint> {

    static AucRocPoint randomPoint() {
        return new AucRocPoint(randomDouble(), randomDouble(), randomDouble());
    }

    @Override
    protected AucRocPoint createTestInstance() {
        return randomPoint();
    }

    @Override
    protected AucRocPoint doParseInstance(XContentParser parser) throws IOException {
        return AucRocPoint.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
