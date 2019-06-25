/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.qa;

/**
 * Interface implemented once per SQL access method to ensure that we
 * test the same minimal set of error cases. Note that this does not
 * include security related failures, those are tracked in another test.
 */
public interface ErrorsTestCase {
    void testSelectInvalidSql() throws Exception;
    void testSelectFromMissingIndex() throws Exception;
    void testSelectFromIndexWithoutTypes() throws Exception;
    void testSelectMissingField() throws Exception;
    void testSelectMissingFunction() throws Exception;
    void testSelectProjectScoreInAggContext() throws Exception;
    void testSelectOrderByScoreInAggContext() throws Exception;
    void testSelectGroupByScore() throws Exception;
    void testSelectScoreSubField() throws Exception;
    void testSelectScoreInScalar() throws Exception;
    void testHardLimitForSortOnAggregate() throws Exception;
}
