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

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class ResultsFieldUpdateTests extends AbstractSerializingTestCase<ResultsFieldUpdate> {

    @Override
    protected ResultsFieldUpdate doParseInstance(XContentParser parser) throws IOException {
        return ResultsFieldUpdate.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<ResultsFieldUpdate> instanceReader() {
        return ResultsFieldUpdate::new;
    }

    @Override
    protected ResultsFieldUpdate createTestInstance() {
        return new ResultsFieldUpdate(randomAlphaOfLength(4));
    }

    public void testIsSupported() {
        ResultsFieldUpdate update = new ResultsFieldUpdate("foo");
        assertTrue(update.isSupported(mock(InferenceConfig.class)));
    }

    public void testApply_OnlyTheResultsFieldIsChanged() {
        if (randomBoolean()) {
            ClassificationConfig config = ClassificationConfigTests.randomClassificationConfig();
            String newResultsField = config.getResultsField() + "foobar";
            ResultsFieldUpdate update = new ResultsFieldUpdate(newResultsField);
            InferenceConfig applied = update.apply(config);

            assertThat(applied, instanceOf(ClassificationConfig.class));
            ClassificationConfig appliedConfig = (ClassificationConfig)applied;
            assertEquals(newResultsField, appliedConfig.getResultsField());

            assertEquals(appliedConfig, new ClassificationConfig.Builder(config).setResultsField(newResultsField).build());
        } else {
            RegressionConfig config = RegressionConfigTests.randomRegressionConfig();
            String newResultsField = config.getResultsField() + "foobar";
            ResultsFieldUpdate update = new ResultsFieldUpdate(newResultsField);
            InferenceConfig applied = update.apply(config);

            assertThat(applied, instanceOf(RegressionConfig.class));
            RegressionConfig appliedConfig = (RegressionConfig)applied;
            assertEquals(newResultsField, appliedConfig.getResultsField());

            assertEquals(appliedConfig, new RegressionConfig.Builder(config).setResultsField(newResultsField).build());
        }
    }
}
