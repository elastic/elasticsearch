/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.test.ESTestCase;

public class EsqlDataTypeConverterTests extends ESTestCase {

    public void testNanoTimeToString() {
        long expected = randomLong();
        long actual = EsqlDataTypeConverter.dateNanosToLong(EsqlDataTypeConverter.nanoTimeToString(expected));
        assertEquals(expected, actual);
    }
}
