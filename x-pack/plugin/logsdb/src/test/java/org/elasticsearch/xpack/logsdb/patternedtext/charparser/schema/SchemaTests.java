/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;

@SuppressForbidden(reason = "prints out the schema for debugging purposes within test")
public class SchemaTests extends ESTestCase {
    public void testReadAndPrintSchema() {
        Schema schema = Schema.getInstance();
        assertNotNull(schema);
        System.out.println(schema);
    }
}
