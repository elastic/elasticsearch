/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap;
import org.junit.BeforeClass;

import java.util.List;

import static java.util.Collections.nCopies;
import static org.hamcrest.Matchers.equalTo;

public class SubqueryRelationClassIT extends AbstractViewSubqueryIntegTestCase {

    @BeforeClass
    public static void requireCapabilities() {
        assumeTrue("requires METADATA_CLASS_AND_NAME", Cap.METADATA_CLASS_AND_NAME.isEnabled());
        assumeTrue("requires SUBQUERY_IN_FROM_COMMAND", Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
    }

    public void testClassAndNameForSingleSubquery() {
        try (var response = run("FROM (FROM languages) METADATA _class, _name | SORT language_code")) {
            assertThat(column(response, "_class"), equalTo(nCopies(4, "subquery")));
            assertThat(column(response, "_name"), equalTo(nCopies(4, null)));
        }
    }

    public void testClassAndNameForTwoSubqueries() {
        try (
            var response = run(
                "FROM (FROM languages | WHERE language_code <= 2), (FROM languages | WHERE language_code >= 3)"
                    + " METADATA _class, _name | SORT language_code"
            )
        ) {
            assertThat(column(response, "_class"), equalTo(nCopies(4, "subquery")));
            assertThat(column(response, "_name"), equalTo(nCopies(4, null)));
        }
    }

    public void testClassAndNameSeparateSubqueryFromIndex() {
        assertThat(
            countsByClassAndName("FROM languages, (FROM languages | KEEP language_code) METADATA _class, _name"),
            equalTo(List.of(row(4L, "index", "languages"), row(4L, "subquery", null)))
        );
    }

    public void testClassAndNameInSubqueryBodyDoNotChangeOuterValues() {
        try (
            var response = run(
                "FROM (FROM languages METADATA _class, _name | RENAME _class AS inner_class, _name AS inner_name)"
                    + " METADATA _class, _name | SORT language_code"
            )
        ) {
            assertThat(column(response, "inner_class"), equalTo(nCopies(4, "index")));
            assertThat(column(response, "inner_name"), equalTo(nCopies(4, "languages")));
            assertThat(column(response, "_class"), equalTo(nCopies(4, "subquery")));
            assertThat(column(response, "_name"), equalTo(nCopies(4, null)));
        }
    }
}
