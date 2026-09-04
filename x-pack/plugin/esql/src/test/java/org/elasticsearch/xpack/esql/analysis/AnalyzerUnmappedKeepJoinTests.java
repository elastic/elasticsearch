/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.type.DataType;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.singleValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

/**
 * KEEP-before-LOOKUP-JOIN grid: mode (load / nullify) × key-mapping (primary / lookup).
 *
 * When a {@code KEEP} command references the join key before the join, the key appears
 * at the Project node level where the lookup's output is not yet in scope.
 * Under {@code load} mode, {@code ResolveUnmapped.load()} fires at that Project node and
 * adds an unmapped primary key as a {@code PotentiallyUnmappedKeywordEsField} — enabling
 * a join that would otherwise fail.
 *
 * Key contrast: row testLoad_unmapped_mapped vs testNullify_unmapped_mapped — identical
 * mapping setup but load fires PotentiallyUnmappedKeyword and succeeds while nullify dies with a left-side error.
 */
public class AnalyzerUnmappedKeepJoinTests extends AnalyzerUnmappedTestBase {

    // -------------------------------------------------------------------------
    // load mode
    // -------------------------------------------------------------------------

    public void testLoad_mapped_mapped_doesNotThrow() {
        test().addLanguagesLookup()
            .statement(
                setUnmappedLoad(
                    "FROM test | EVAL language_code = languages | KEEP language_code | LOOKUP JOIN languages_lookup ON language_code"
                )
            );
    }

    public void testLoad_mapped_unmapped_rightSideError() {
        test().addLanguagesLookup()
            .statementError(
                setUnmappedLoad("FROM test | KEEP emp_no | LOOKUP JOIN languages_lookup ON emp_no"),
                containsString("Unknown column [emp_no] in right side of join")
            );
    }

    public void testLoad_unmapped_mapped_pukFires() {
        // KEEP forces load() to fire at the Project node before the join — message becomes PotentiallyUnmappedKeyword (KEYWORD).
        assumeTrue(
            "requires optional_fields_load_with_lookup_join",
            EsqlCapabilities.Cap.OPTIONAL_FIELDS_LOAD_WITH_LOOKUP_JOIN.isEnabled()
        );
        var plan = test().addLookupIndex("message_lookup", messageLookupIndex())
            .statement(setUnmappedLoad("FROM test | KEEP message | LOOKUP JOIN message_lookup ON message"));
        var message = singleValue(plan.output().stream().filter(a -> "message".equals(a.name())).toList());
        assertThat(message.dataType(), is(DataType.KEYWORD));
    }

    public void testLoad_unmapped_unmapped_rightSideError() {
        // load() rescues left at KEEP; right side has no such field → error.
        test().addLanguagesLookup()
            .statementError(
                setUnmappedLoad("FROM test | KEEP does_not_exist | LOOKUP JOIN languages_lookup ON does_not_exist"),
                containsString("Unknown column [does_not_exist] in right side of join")
            );
    }

    // -------------------------------------------------------------------------
    // nullify mode — no PotentiallyUnmappedKeyword is inserted, so outcomes differ only for unmapped primary keys
    // -------------------------------------------------------------------------

    public void testNullify_mapped_mapped_doesNotThrow() {
        test().addLanguagesLookup()
            .statement(
                setUnmappedNullify(
                    "FROM test | EVAL language_code = languages | KEEP language_code | LOOKUP JOIN languages_lookup ON language_code"
                )
            );
    }

    public void testNullify_mapped_unmapped_rightSideError() {
        test().addLanguagesLookup()
            .statementError(
                setUnmappedNullify("FROM test | KEEP emp_no | LOOKUP JOIN languages_lookup ON emp_no"),
                containsString("Unknown column [emp_no] in right side of join")
            );
    }

    public void testNullify_unmapped_mapped_nullifiedKey_doesNotThrow() {
        // KEEP places the key at a Project before the join; nullify fires there → key = NULL.
        // Analysis succeeds but no rows match at runtime (contrast with load which uses PotentiallyUnmappedKeyword).
        test().addLanguagesLookup()
            .statement(setUnmappedNullify("FROM test | KEEP language_code | LOOKUP JOIN languages_lookup ON language_code"));
    }

    public void testNullify_unmapped_unmapped_error() {
        // nullify resolves left at KEEP; right side has no such field → error.
        test().addLanguagesLookup()
            .statementError(
                setUnmappedNullify("FROM test | KEEP does_not_exist | LOOKUP JOIN languages_lookup ON does_not_exist"),
                containsString("Unknown column [does_not_exist] in right side of join")
            );
    }
}
