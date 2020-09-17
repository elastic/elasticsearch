/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.test.ESTestCase;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class EvaluationFieldsTests extends ESTestCase {

    public void testConstructorAndGetters() {
        EvaluationFields fields = new EvaluationFields("a", "b", "c", "d", "e");
        assertThat(fields.getActualField(), is(equalTo("a")));
        assertThat(fields.getPredictedField(), is(equalTo("b")));
        assertThat(fields.getTopClassesField(), is(equalTo("c")));
        assertThat(fields.getPredictedClassField(), is(equalTo("d")));
        assertThat(fields.getPredictedProbabilityField(), is(equalTo("e")));
    }

    public void testConstructorAndGetters_WithNullValues() {
        EvaluationFields fields = new EvaluationFields("a", null, "c", null, "e");
        assertThat(fields.getActualField(), is(equalTo("a")));
        assertThat(fields.getPredictedField(), is(nullValue()));
        assertThat(fields.getTopClassesField(), is(equalTo("c")));
        assertThat(fields.getPredictedClassField(), is(nullValue()));
        assertThat(fields.getPredictedProbabilityField(), is(equalTo("e")));
    }

    public void testListPotentiallyRequiredFields() {
        EvaluationFields fields = new EvaluationFields("a", "b", "c", "d", "e");
        assertThat(fields.listPotentiallyRequiredFields().stream().map(Tuple::v2).collect(toList()), contains("a", "b", "d", "e"));
    }

    public void testListPotentiallyRequiredFields_WithNullValues() {
        EvaluationFields fields = new EvaluationFields("a", null, "c", null, "e");
        assertThat(fields.listPotentiallyRequiredFields().stream().map(Tuple::v2).collect(toList()), contains("a", null, null, "e"));
    }
}
