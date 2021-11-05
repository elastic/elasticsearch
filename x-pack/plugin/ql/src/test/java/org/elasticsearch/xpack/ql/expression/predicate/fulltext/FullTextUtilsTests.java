/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.predicate.fulltext;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.ParsingException;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Map;

import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

public class FullTextUtilsTests extends ESTestCase {

    private final Source source = new Source(1, 1, "");

    public void testColonDelimited() {
        Map<String, String> options = FullTextUtils.parseSettings("k1=v1;k2=v2", source);
        assertThat(options.size(), is(2));
        assertThat(options, hasEntry("k1", "v1"));
        assertThat(options, hasEntry("k2", "v2"));
    }

    public void testColonDelimitedErrorString() {
        ParsingException e = expectThrows(ParsingException.class, () -> FullTextUtils.parseSettings("k1=v1;k2v2", source));
        assertThat(e.getMessage(), is("line 1:3: Cannot parse entry k2v2 in options k1=v1;k2v2"));
        assertThat(e.getLineNumber(), is(1));
        assertThat(e.getColumnNumber(), is(3));
    }

    public void testColonDelimitedErrorDuplicate() {
        ParsingException e = expectThrows(ParsingException.class, () -> FullTextUtils.parseSettings("k1=v1;k1=v2", source));
        assertThat(e.getMessage(), is("line 1:3: Duplicate option k1=v2 detected in options k1=v1;k1=v2"));
        assertThat(e.getLineNumber(), is(1));
        assertThat(e.getColumnNumber(), is(3));
    }
}
