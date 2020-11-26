/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.analyses;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class FieldCardinalityConstraintTests extends ESTestCase {

    public void testBetween_GivenWithinLimits() {
        FieldCardinalityConstraint constraint = FieldCardinalityConstraint.between("foo", 3, 6);

        constraint.check(3);
        constraint.check(4);
        constraint.check(5);
        constraint.check(6);
    }

    public void testBetween_GivenLessThanLowerBound() {
        FieldCardinalityConstraint constraint = FieldCardinalityConstraint.between("foo", 3, 6);

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> constraint.check(2L));
        assertThat(e.getMessage(), equalTo("Field [foo] must have at least [3] distinct values but there were [2]"));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
    }

    public void testBetween_GivenGreaterThanUpperBound() {
        FieldCardinalityConstraint constraint = FieldCardinalityConstraint.between("foo", 3, 6);

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> constraint.check(7L));
        assertThat(e.getMessage(), equalTo("Field [foo] must have at most [6] distinct values but there were at least [7]"));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
    }
}
