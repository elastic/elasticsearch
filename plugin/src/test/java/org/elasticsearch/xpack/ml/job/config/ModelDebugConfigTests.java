/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.support.AbstractSerializingTestCase;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ModelDebugConfigTests extends AbstractSerializingTestCase<ModelDebugConfig> {

    public void testConstructorDefaults() {
        assertThat(new ModelDebugConfig().isEnabled(), is(true));
        assertThat(new ModelDebugConfig().getTerms(), is(nullValue()));
    }

    @Override
    protected ModelDebugConfig createTestInstance() {
        return new ModelDebugConfig(randomBoolean(), randomAsciiOfLengthBetween(1, 30));
    }

    @Override
    protected Reader<ModelDebugConfig> instanceReader() {
        return ModelDebugConfig::new;
    }

    @Override
    protected ModelDebugConfig parseInstance(XContentParser parser) {
        return ModelDebugConfig.PARSER.apply(parser, null);
    }
}
