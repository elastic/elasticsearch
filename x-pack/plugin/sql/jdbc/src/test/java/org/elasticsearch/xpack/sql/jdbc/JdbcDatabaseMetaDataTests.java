/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.jdbc.JdbcDatabaseMetaData;

public class JdbcDatabaseMetaDataTests extends ESTestCase {

    private JdbcDatabaseMetaData md = new JdbcDatabaseMetaData(null);

    public void testSeparators() throws Exception {
        assertEquals(":", md.getCatalogSeparator());
        assertEquals("\"", md.getIdentifierQuoteString());
        assertEquals("\\", md.getSearchStringEscape());
        
    }
}
