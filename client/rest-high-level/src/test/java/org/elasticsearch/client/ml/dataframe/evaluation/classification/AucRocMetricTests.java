/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe.evaluation.classification;

import org.elasticsearch.client.ml.dataframe.evaluation.MlEvaluationNamedXContentProvider;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class AucRocMetricTests extends AbstractXContentTestCase<AucRocMetric> {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlEvaluationNamedXContentProvider().getNamedXContentParsers());
    }

    public static AucRocMetric createRandom() {
        return new AucRocMetric(
            randomAlphaOfLengthBetween(1, 10),
            randomBoolean() ? randomBoolean() : null);
    }

    @Override
    protected AucRocMetric createTestInstance() {
        return createRandom();
    }

    @Override
    protected AucRocMetric doParseInstance(XContentParser parser) throws IOException {
        return AucRocMetric.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
