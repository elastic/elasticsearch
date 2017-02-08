/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.results;

import org.elasticsearch.test.ESTestCase;

public class ReservedFieldNamesTests extends ESTestCase {

    public void testIsValidFieldName() throws Exception {
        assertTrue(ReservedFieldNames.isValidFieldName("host"));
        assertTrue(ReservedFieldNames.isValidFieldName("host.actual"));
        assertFalse(ReservedFieldNames.isValidFieldName("actual.host"));
        assertFalse(ReservedFieldNames.isValidFieldName(AnomalyRecord.BUCKET_SPAN.getPreferredName()));
    }

}