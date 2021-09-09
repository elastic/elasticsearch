/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.results;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.util.ArrayList;
import java.util.List;

public class InfluenceTests extends AbstractXContentTestCase<Influence> {

    @Override
    protected Influence createTestInstance() {
        int size = randomInt(10);
        List<String> fieldValues = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            fieldValues.add(randomAlphaOfLengthBetween(1, 20));
        }
        return new Influence(randomAlphaOfLengthBetween(1, 30), fieldValues);
    }

    @Override
    protected Influence doParseInstance(XContentParser parser) {
        return Influence.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
