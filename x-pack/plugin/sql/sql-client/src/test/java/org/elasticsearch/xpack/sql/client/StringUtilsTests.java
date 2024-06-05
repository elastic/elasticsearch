/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.client;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.sql.client.StringUtils.nullAsEmpty;

public class StringUtilsTests extends ESTestCase {
    public void testNullAsEmpty() {
        assertEquals("", nullAsEmpty(null));
        assertEquals("", nullAsEmpty(""));
        String rando = randomRealisticUnicodeOfCodepointLength(5);
        assertEquals(rando, nullAsEmpty(rando));
    }
}
