/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.parser;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.tree.Location;

import static org.hamcrest.Matchers.is;

public class IdentifierBuilderTests extends ESTestCase {

    private static Location L = new Location(1, 10);

    public void testTypicalIndex() throws Exception {
        IdentifierBuilder.validateIndex("some-index", L);
    }

    public void testInternalIndex() throws Exception {
        IdentifierBuilder.validateIndex(".some-internal-index-2020-02-02", L);
    }

    public void testIndexPattern() throws Exception {
        IdentifierBuilder.validateIndex(".some-*", L);
    }

    public void testInvalidIndex() throws Exception {
        ParsingException pe = expectThrows(ParsingException.class, () -> IdentifierBuilder.validateIndex("some,index", L));
        assertThat(pe.getMessage(), is("line 1:12: Invalid index name (illegal character ,) some,index"));
    }

    public void testUpperCasedIndex() throws Exception {
        ParsingException pe = expectThrows(ParsingException.class, () -> IdentifierBuilder.validateIndex("thisIsAnIndex", L));
        assertThat(pe.getMessage(), is("line 1:12: Invalid index name (needs to be lowercase) thisIsAnIndex"));
    }
}
