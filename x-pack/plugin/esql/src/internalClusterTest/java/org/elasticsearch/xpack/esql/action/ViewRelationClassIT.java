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

public class ViewRelationClassIT extends AbstractViewSubqueryIntegTestCase {

    @BeforeClass
    public static void requireCapabilities() {
        assumeTrue("requires METADATA_CLASS_AND_NAME", Cap.METADATA_CLASS_AND_NAME.isEnabled());
        assumeTrue("requires VIEWS_WITH_NO_BRANCHING", Cap.VIEWS_WITH_NO_BRANCHING.isEnabled());
    }

    public void testClassAndNameForSingleView() {
        createView("view_langs_it", "FROM languages");
        try (var response = run("FROM view_langs_it METADATA _class, _name | SORT language_code")) {
            assertThat(column(response, "_class"), equalTo(nCopies(4, "view")));
            assertThat(column(response, "_name"), equalTo(nCopies(4, "view_langs_it")));
        }
        // _class alone must also keep a pass-through view from being inlined into its index.
        try (var response = run("FROM view_langs_it METADATA _class | SORT language_code")) {
            assertThat(column(response, "_class"), equalTo(nCopies(4, "view")));
        }
    }

    public void testClassAndNameViaWildcardPattern() {
        createView("view_langs_wildcard_it", "FROM languages");
        try (var response = run("FROM view_langs_wildcard_it METADATA _cl*, _na* | SORT language_code")) {
            assertThat(column(response, "_class"), equalTo(nCopies(4, "view")));
            assertThat(column(response, "_name"), equalTo(nCopies(4, "view_langs_wildcard_it")));
        }
    }

    public void testClassAnsweredWhileIndexNullFilledOnView() {
        createView("view_langs_nullfill_it", "FROM languages");
        try (var response = run("FROM view_langs_nullfill_it METADATA _class, _name, _index | SORT language_code")) {
            assertThat(column(response, "_class"), equalTo(nCopies(4, "view")));
            assertThat(column(response, "_name"), equalTo(nCopies(4, "view_langs_nullfill_it")));
            assertThat(column(response, "_index"), equalTo(nCopies(4, null)));
        }
    }

    public void testClassAndNameOnViewEndingInWildcardKeep() {
        createView("view_langs_keep_star_it", "FROM languages | KEEP *");
        try (var response = run("FROM view_langs_keep_star_it METADATA _class, _name | SORT language_code")) {
            assertThat(column(response, "_class"), equalTo(nCopies(4, "view")));
            assertThat(column(response, "_name"), equalTo(nCopies(4, "view_langs_keep_star_it")));
        }
    }

    public void testOuterWildcardKeepExposesClassAndName() {
        createView("view_langs_outer_star_it", "FROM languages");
        List<String> indexColumns;
        try (var response = run("FROM languages METADATA _class, _name | KEEP * | SORT language_code")) {
            indexColumns = response.columns().stream().map(ColumnInfoImpl::name).toList();
        }
        try (var response = run("FROM view_langs_outer_star_it METADATA _class, _name | KEEP * | SORT language_code")) {
            assertThat(response.columns().stream().map(ColumnInfoImpl::name).toList(), equalTo(indexColumns));
            assertThat(column(response, "_class"), equalTo(nCopies(4, "view")));
            assertThat(column(response, "_name"), equalTo(nCopies(4, "view_langs_outer_star_it")));
        }
    }

    public void testClassAndNameViewBesideIndex() {
        assumeTrue("requires VIEWS_WITH_BRANCHING", Cap.VIEWS_WITH_BRANCHING.isEnabled());
        createView("view_langs_beside_it", "FROM languages | WHERE language_code <= 2");

        assertThat(
            countsByClassAndName("FROM languages, view_langs_beside_it METADATA _class, _name"),
            equalTo(List.of(row(4L, "index", "languages"), row(2L, "view", "view_langs_beside_it")))
        );
    }

    public void testNestedViewInnerBodyClassAndNameOuterWins() {
        createView("view_langs_inner_class_it", "FROM languages METADATA _class, _name");
        createView("view_langs_outer_class_it", "FROM view_langs_inner_class_it");

        try (var response = run("FROM view_langs_outer_class_it | SORT language_code")) {
            assertThat(column(response, "_class"), equalTo(nCopies(4, "index")));
            assertThat(column(response, "_name"), equalTo(nCopies(4, "languages")));
        }
        try (var response = run("FROM view_langs_outer_class_it METADATA _class, _name | SORT language_code")) {
            assertThat(column(response, "_class"), equalTo(nCopies(4, "view")));
            assertThat(column(response, "_name"), equalTo(nCopies(4, "view_langs_outer_class_it")));
        }
    }

    public void testViewAndSubqueryBodiesDeclareClassAndNameOuterWins() {
        assumeTrue("requires VIEWS_WITH_BRANCHING", Cap.VIEWS_WITH_BRANCHING.isEnabled());
        createView("view_langs_both_bodies_it", "FROM languages METADATA _class, _name");

        assertThat(
            countsByClassAndName(
                "FROM view_langs_both_bodies_it, (FROM languages METADATA _class, _name | WHERE language_code <= 2) METADATA _class, _name"
            ),
            equalTo(List.of(row(2L, "subquery", null), row(4L, "view", "view_langs_both_bodies_it")))
        );
    }

    public void testViewWithBranchingBodyAndTrailingEvalAnswersTheView() {
        assumeTrue("requires VIEWS_WITH_BRANCHING", Cap.VIEWS_WITH_BRANCHING.isEnabled());
        createView(
            "view_langs_union_eval_it",
            "FROM languages, (FROM languages | WHERE language_code <= 2) | EVAL doubled = language_code * 2"
        );

        assertThat(
            countsByClassAndName("FROM view_langs_union_eval_it METADATA _class, _name"),
            equalTo(List.of(row(6L, "view", "view_langs_union_eval_it")))
        );
    }

    public void testViewWithBareUnionBodyAnswersTheViewWhenFlattened() {
        assumeTrue("requires VIEWS_WITH_BRANCHING", Cap.VIEWS_WITH_BRANCHING.isEnabled());
        createView("view_langs_union_it", "FROM languages, (FROM languages | WHERE language_code <= 2)");

        assertThat(
            countsByClassAndName("FROM view_langs_union_it METADATA _class, _name"),
            equalTo(List.of(row(6L, "view", "view_langs_union_it")))
        );
        assertThat(
            countsByClassAndName("FROM view_langs_union_it, languages METADATA _class, _name"),
            equalTo(List.of(row(4L, "index", "languages"), row(6L, "view", "view_langs_union_it")))
        );
    }

    public void testViewOfViewsAnswersTheOuterView() {
        assumeTrue("requires VIEWS_WITH_BRANCHING", Cap.VIEWS_WITH_BRANCHING.isEnabled());
        createView("view_langs_low_it", "FROM languages | WHERE language_code <= 2");
        createView("view_langs_high_it", "FROM languages | WHERE language_code > 2");
        createView("view_langs_of_views_it", "FROM view_langs_low_it, view_langs_high_it");

        assertThat(
            countsByClassAndName("FROM view_langs_of_views_it METADATA _class, _name"),
            equalTo(List.of(row(4L, "view", "view_langs_of_views_it")))
        );
        assertThat(
            countsByClassAndName("FROM view_langs_of_views_it, languages METADATA _class, _name"),
            equalTo(List.of(row(4L, "index", "languages"), row(4L, "view", "view_langs_of_views_it")))
        );
    }

    public void testSubqueryAroundViewsAnswersSubquery() {
        assumeTrue("requires VIEWS_WITH_BRANCHING", Cap.VIEWS_WITH_BRANCHING.isEnabled());
        createView("view_langs_low_it", "FROM languages | WHERE language_code <= 2");
        createView("view_langs_high_it", "FROM languages | WHERE language_code > 2");

        assertThat(
            countsByClassAndName("FROM (FROM view_langs_low_it) METADATA _class, _name"),
            equalTo(List.of(row(2L, "subquery", null)))
        );
        assertThat(
            countsByClassAndName("FROM languages, (FROM view_langs_low_it) METADATA _class, _name"),
            equalTo(List.of(row(4L, "index", "languages"), row(2L, "subquery", null)))
        );
        assertThat(
            countsByClassAndName("FROM languages, (FROM view_langs_low_it, view_langs_high_it) METADATA _class, _name"),
            equalTo(List.of(row(4L, "index", "languages"), row(4L, "subquery", null)))
        );
    }

    public void testViewBodyAsksClassAndNameOfItsUnionBodyView() {
        assumeTrue("requires VIEWS_WITH_BRANCHING", Cap.VIEWS_WITH_BRANCHING.isEnabled());
        createView("view_langs_union_it", "FROM languages, (FROM languages | WHERE language_code <= 2)");
        createView("view_langs_asks_it", "FROM view_langs_union_it, languages METADATA _class, _name");

        assertThat(
            countsByClassAndName("FROM view_langs_asks_it"),
            equalTo(List.of(row(4L, "index", "languages"), row(6L, "view", "view_langs_union_it")))
        );
    }
}
