/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.sql.jdbc.JdbcConfiguration.EscapeWildcard.ALWAYS;
import static org.elasticsearch.xpack.sql.jdbc.JdbcConfiguration.EscapeWildcard.AUTO;
import static org.elasticsearch.xpack.sql.jdbc.JdbcConfiguration.EscapeWildcard.NEVER;
import static org.elasticsearch.xpack.sql.jdbc.JdbcDatabaseMetaData.escapeStringPattern;

public class JdbcDatabaseMetaDataTests extends ESTestCase {

    private JdbcDatabaseMetaData md = new JdbcDatabaseMetaData(null);

    public void testSeparators() throws Exception {
        assertEquals(":", md.getCatalogSeparator());
        assertEquals("\"", md.getIdentifierQuoteString());
        assertEquals("\\", md.getSearchStringEscape());
    }

    public void testEscapeWildcardNever() {
        assertEquals("foo", escapeStringPattern("foo", NEVER));
        assertEquals("f__", escapeStringPattern("f__", NEVER));
        assertEquals("f__b", escapeStringPattern("f__b", NEVER));
        assertEquals("f%_b", escapeStringPattern("f%_b", NEVER));
        assertEquals("f\\_b", escapeStringPattern("f\\_b", NEVER));
        assertEquals(null, escapeStringPattern(null, NEVER));
        assertEquals("f\\_b\\", escapeStringPattern("f\\_b\\", NEVER));
    }

    public void testEscapeWildcardAlways() {
        assertEquals("foo", escapeStringPattern("foo", ALWAYS));
        assertEquals("%", escapeStringPattern("%", ALWAYS));
        assertEquals("\\%a", escapeStringPattern("%a", ALWAYS));
        assertEquals("f\\_", escapeStringPattern("f_", ALWAYS));
        assertEquals("f\\\\_", escapeStringPattern("f\\_", ALWAYS));
        assertEquals("f\\%\\_", escapeStringPattern("f%_", ALWAYS));
        assertEquals("f\\_b\\", escapeStringPattern("f\\_b\\", ALWAYS));
    }

    public void testEscapeWildcardAuto() {
        assertEquals("foo", escapeStringPattern("foo", AUTO));
        assertEquals("%", escapeStringPattern("%", AUTO));
        assertEquals("%a", escapeStringPattern("%a", AUTO));
        assertEquals("f\\_", escapeStringPattern("f_", AUTO));
        assertEquals("f\\_", escapeStringPattern("f\\_", AUTO));
        assertEquals("f%\\_", escapeStringPattern("f%_", AUTO));
        assertEquals("f%\\_", escapeStringPattern("f%\\_", AUTO));
        assertEquals("%foo\\_", escapeStringPattern("%foo\\_", AUTO));
        assertEquals("%foo\\_", escapeStringPattern("%foo_", AUTO));
        assertEquals("\\_foo\\_", escapeStringPattern("_foo_", AUTO));
        assertEquals("f_b\\", escapeStringPattern("f\\_b\\", AUTO));
    }
}
