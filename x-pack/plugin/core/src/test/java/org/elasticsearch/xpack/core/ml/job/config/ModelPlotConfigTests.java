/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ModelPlotConfigTests extends AbstractSerializingTestCase<ModelPlotConfig> {

    public void testConstructorDefaults() {
        ModelPlotConfig modelPlotConfig = new ModelPlotConfig();
        assertThat(modelPlotConfig.isEnabled(), is(true));
        assertThat(modelPlotConfig.getTerms(), is(nullValue()));
        assertThat(modelPlotConfig.annotationsEnabled(), is(true));
    }

    public void testAnnotationEnabledDefaultsToEnabled() {
        ModelPlotConfig modelPlotConfig = new ModelPlotConfig(false, null, null);
        assertThat(modelPlotConfig.annotationsEnabled(), is(false));

        modelPlotConfig = new ModelPlotConfig(true, null, null);
        assertThat(modelPlotConfig.annotationsEnabled(), is(true));
    }

    @Override
    protected ModelPlotConfig createTestInstance() {
        return createRandomized();
    }

    public static ModelPlotConfig createRandomized() {
        return new ModelPlotConfig(randomBoolean(), randomAlphaOfLengthBetween(1, 30), randomBoolean() ? randomBoolean() : null);
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
