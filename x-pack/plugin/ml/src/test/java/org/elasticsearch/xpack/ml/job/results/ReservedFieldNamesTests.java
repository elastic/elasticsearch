/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.results;

import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.core.ml.job.results.ReservedFieldNames;

public class ReservedFieldNamesTests extends ESTestCase {

    public void testIsValidFieldName() {
        assertTrue(ReservedFieldNames.isValidFieldName("host"));
        assertTrue(ReservedFieldNames.isValidFieldName("host.actual"));
        assertFalse(ReservedFieldNames.isValidFieldName("actual.host"));
        assertFalse(ReservedFieldNames.isValidFieldName(AnomalyRecord.BUCKET_SPAN.getPreferredName()));
        assertFalse(ReservedFieldNames.isValidFieldName(GetResult._INDEX));
        assertFalse(ReservedFieldNames.isValidFieldName(GetResult._ID));
    }
}
