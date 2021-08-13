/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.unit.Fuzziness;

import static org.hamcrest.Matchers.equalTo;

abstract class AbstractNonTextScriptFieldTypeTestCase extends AbstractScriptFieldTypeTestCase {
    public void testFuzzyQueryIsError() {
        assertQueryOnlyOnTextAndKeyword(
            "fuzzy",
            () -> simpleMappedFieldType().fuzzyQuery("cat", Fuzziness.AUTO, 0, 1, true, mockContext())
        );
    }

    public void testPrefixQueryIsError() {
        assertQueryOnlyOnTextKeywordAndWildcard("prefix", () -> simpleMappedFieldType().prefixQuery("cat", null, mockContext()));
    }

    public void testRegexpQueryIsError() {
        assertQueryOnlyOnTextAndKeyword(
            "regexp",
            () -> simpleMappedFieldType().regexpQuery("cat", 0, 0, Operations.DEFAULT_MAX_DETERMINIZED_STATES, null, mockContext())
        );
    }

    public void testWildcardQueryIsError() {
        assertQueryOnlyOnTextKeywordAndWildcard("wildcard", () -> simpleMappedFieldType().wildcardQuery("cat", null, mockContext()));
    }

    private void assertQueryOnlyOnTextAndKeyword(String queryName, ThrowingRunnable buildQuery) {
        Exception e = expectThrows(IllegalArgumentException.class, buildQuery);
        assertThat(
            e.getMessage(),
            equalTo(
                "Can only use "
                    + queryName
                    + " queries on keyword and text fields - not on [test] which is a runtime field of type ["
                    + typeName()
                    + "]"
            )
        );
    }

    private void assertQueryOnlyOnTextKeywordAndWildcard(String queryName, ThrowingRunnable buildQuery) {
        Exception e = expectThrows(IllegalArgumentException.class, buildQuery);
        assertThat(
            e.getMessage(),
            equalTo(
                "Can only use "
                    + queryName
                    + " queries on keyword, text and wildcard fields - not on [test] which is a runtime field of type ["
                    + typeName()
                    + "]"
            )
        );
    }
}
