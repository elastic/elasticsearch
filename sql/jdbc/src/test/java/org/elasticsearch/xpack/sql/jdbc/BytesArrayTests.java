/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.jdbc.util.BytesArray;

public class BytesArrayTests extends ESTestCase {

    public void testToString() {
        assertEquals("01020af6", new BytesArray(new byte[] {1, 2, 10, -10}).toString());
        assertEquals("", new BytesArray(new byte[] {}).toString());
    }

}
