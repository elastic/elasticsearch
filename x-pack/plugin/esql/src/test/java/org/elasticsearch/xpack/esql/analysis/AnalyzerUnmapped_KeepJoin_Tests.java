/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.type.DataType;

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
 * mapping setup but load fires PUK and succeeds while nullify dies with a left-side error.
 */
public class AnalyzerUnmapped_KeepJoin_Tests extends AnalyzerUnmappedTestBase {

    // -------------------------------------------------------------------------
    // load mode
    // -------------------------------------------------------------------------

    public void testLoad_mapped_mapped_succeeds() {
        // EVAL creates language_code in the primary output; KEEP + join proceed normally.
        test().addLanguagesLookup()
            .statement(
                setUnmappedLoad(
                    "FROM test | EVAL language_code = languages | KEEP language_code | LOOKUP JOIN languages_lookup ON language_code"
                )
            );
    }

    public void testLoad_mapped_unmapped_rightSideError() {
        // emp_no is mapped in primary; languages_lookup has no emp_no — KEEP does not help the right side.
        test().addLanguagesLookup()
            .statementError(
                setUnmappedLoad("FROM test | KEEP emp_no | LOOKUP JOIN languages_lookup ON emp_no"),
                containsString("Unknown column [emp_no] in right side of join")
            );
    }

    public void testLoad_unmapped_mapped_pukFires_succeeds() {
        // message is absent from the primary but present in message_lookup.
        // KEEP places message at the Project node before the join resolves the lookup output.
        // load() fires at that Project, adds message as PUK from primary _source.
        // The join then proceeds and the key type in the output is KEYWORD.
        assumeTrue(
            "requires optional_fields_load_with_lookup_join",
            EsqlCapabilities.Cap.OPTIONAL_FIELDS_LOAD_WITH_LOOKUP_JOIN.isEnabled()
        );
        var plan = test().addLookupIndex("message_lookup", messageLookupIndex())
            .statement(setUnmappedLoad("FROM test | KEEP message | LOOKUP JOIN message_lookup ON message"));
        var message = plan.output().stream().filter(a -> "message".equals(a.name())).findFirst().orElseThrow();
        assertThat(message.dataType(), is(DataType.KEYWORD));
    }

    public void testLoad_unmapped_unmapped_rightSideError() {
        // load() fires at KEEP and adds does_not_exist as PUK on the primary side.
        // The lookup has no does_not_exist field and load() skips LOOKUP EsRelations → right side error.
        test().addLanguagesLookup()
            .statementError(
                setUnmappedLoad("FROM test | KEEP does_not_exist | LOOKUP JOIN languages_lookup ON does_not_exist"),
                containsString("Unknown column [does_not_exist] in right side of join")
            );
    }

    // -------------------------------------------------------------------------
    // nullify mode — no PUK is inserted, so outcomes differ only for unmapped primary keys
    // -------------------------------------------------------------------------

    public void testNullify_mapped_mapped_succeeds() {
        // Same outcome as the load counterpart — key is mapped, KEEP + join work regardless of mode.
        test().addLanguagesLookup()
            .statement(
                setUnmappedNullify(
                    "FROM test | EVAL language_code = languages | KEEP language_code | LOOKUP JOIN languages_lookup ON language_code"
                )
            );
    }

    public void testNullify_mapped_unmapped_rightSideError() {
        // Same outcome as load counterpart — right side missing regardless of mode.
        test().addLanguagesLookup()
            .statementError(
                setUnmappedNullify("FROM test | KEEP emp_no | LOOKUP JOIN languages_lookup ON emp_no"),
                containsString("Unknown column [emp_no] in right side of join")
            );
    }

    public void testNullify_unmapped_mapped_nullifiedKey_succeeds() {
        // language_code is absent from primary. The KEEP places it in the Project node *before* the
        // join; the nullify pass inserts Eval(language_code = NULL) at that point, making the field
        // available. Analysis succeeds, but the join key is null so no rows match at runtime.
        //
        // Contrast 1: WITHOUT KEEP (Cat 1 test), the same combo errors with "Unknown column in left
        // side of join" — the nullify pass doesn't fire because there's no Project above the join.
        // Contrast 2: testLoad_unmapped_mapped_pukFires_succeeds — same mapping, load fires PUK so
        // the key is KEYWORD and the join actually produces data.
        test().addLanguagesLookup()
            .statement(setUnmappedNullify("FROM test | KEEP language_code | LOOKUP JOIN languages_lookup ON language_code"));
    }

    public void testNullify_unmapped_unmapped_error() {
        // does_not_exist absent from both; nullify produces no PUK; error before or at the join.
        test().addLanguagesLookup()
            .statementError(
                setUnmappedNullify("FROM test | KEEP does_not_exist | LOOKUP JOIN languages_lookup ON does_not_exist"),
                containsString("Unknown column [does_not_exist]")
            );
    }
}
