/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class EvaluationParametersTests extends ESTestCase {

    public void testConstructorAndGetters() {
        EvaluationParameters params = new EvaluationParameters(17);
        assertThat(params.getMaxBuckets(), equalTo(17));
    }
}
