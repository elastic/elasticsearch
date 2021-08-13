/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.config;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

public class ModelPlotConfigTests extends AbstractXContentTestCase<ModelPlotConfig> {

    @Override
    protected ModelPlotConfig createTestInstance() {
        return createRandomized();
    }

    public static ModelPlotConfig createRandomized() {
        return new ModelPlotConfig(randomBoolean(), randomAlphaOfLengthBetween(1, 30), randomBoolean() ? randomBoolean() : null);
    }

    @Override
    protected ModelPlotConfig doParseInstance(XContentParser parser) {
        return ModelPlotConfig.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
