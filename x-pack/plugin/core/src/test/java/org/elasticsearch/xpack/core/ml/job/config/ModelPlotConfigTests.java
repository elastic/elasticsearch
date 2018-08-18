/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ModelPlotConfigTests extends AbstractSerializingTestCase<ModelPlotConfig> {

    public void testConstructorDefaults() {
        assertThat(new ModelPlotConfig().isEnabled(), is(true));
        assertThat(new ModelPlotConfig().getTerms(), is(nullValue()));
    }

    @Override
    protected ModelPlotConfig createTestInstance() {
        return new ModelPlotConfig(randomBoolean(), randomAlphaOfLengthBetween(1, 30));
    }

    @Override
    protected Reader<ModelPlotConfig> instanceReader() {
        return ModelPlotConfig::new;
    }

    @Override
    protected ModelPlotConfig doParseInstance(XContentParser parser) {
        return ModelPlotConfig.STRICT_PARSER.apply(parser, null);
    }
}
