/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.analyses;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class OutlierDetectionTests extends AbstractSerializingTestCase<OutlierDetection> {

    @Override
    protected OutlierDetection doParseInstance(XContentParser parser) throws IOException {
        return OutlierDetection.fromXContent(parser, false);
    }

    @Override
    protected OutlierDetection createTestInstance() {
        return createRandom();
    }

    public static OutlierDetection createRandom() {
        Integer numberNeighbors = randomBoolean() ? null : randomIntBetween(1, 20);
        OutlierDetection.Method method = randomBoolean() ? null : randomFrom(OutlierDetection.Method.values());
        return new OutlierDetection(numberNeighbors, method);
    }

    @Override
    protected Writeable.Reader<OutlierDetection> instanceReader() {
        return OutlierDetection::new;
    }
}
