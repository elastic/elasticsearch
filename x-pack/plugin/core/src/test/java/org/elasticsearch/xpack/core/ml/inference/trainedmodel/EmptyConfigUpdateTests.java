/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.sameInstance;

public class EmptyConfigUpdateTests extends AbstractSerializingTestCase<EmptyConfigUpdate> {
    @Override
    protected EmptyConfigUpdate doParseInstance(XContentParser parser) throws IOException {
        return EmptyConfigUpdate.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<EmptyConfigUpdate> instanceReader() {
        return EmptyConfigUpdate::new;
    }

    @Override
    protected EmptyConfigUpdate createTestInstance() {
        return new EmptyConfigUpdate();
    }

    public void testIsSupported() {
        InferenceConfig config = randomBoolean() ? ClassificationConfigTests.randomClassificationConfig()
            : RegressionConfigTests.randomRegressionConfig();

        EmptyConfigUpdate update = new EmptyConfigUpdate();
        assertTrue(update.isSupported(config));
    }

    public void testApply() {
        InferenceConfig config = randomBoolean() ? ClassificationConfigTests.randomClassificationConfig()
            : RegressionConfigTests.randomRegressionConfig();

        EmptyConfigUpdate update = new EmptyConfigUpdate();
        assertThat(config, sameInstance(update.apply(config)));
    }
}
