/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.core.type.DataType;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.singleValue;
import static org.hamcrest.Matchers.is;

/**
 * ENRICH + unmapped_fields mode tests.
 *
 * Unlike LOOKUP JOIN, ENRICH has no bilateral key resolution: {@code load()} is orthogonal
 * to the ENRICH itself and simply adds PotentiallyUnmappedKeyword fields to the primary EsRelation when needed.
 */
public class AnalyzerUnmappedEnrichTests extends AnalyzerUnmappedTestBase {

    private static TestAnalyzer testWithLanguagesEnrich() {
        return test().addEnrichPolicy(EnrichPolicy.MATCH_TYPE, "languages", "language_code", "languages_idx", "mapping-languages.json");
    }

    public void testLoad_allMapped_doesNotThrow() {
        // Baseline: all fields mapped, ENRICH works normally under load mode.
        testWithLanguagesEnrich().statement(
            setUnmappedLoad("FROM test | EVAL language_code = languages | ENRICH languages ON language_code | KEEP emp_no, language_name")
        );
    }

    public void testLoad_mappedKey_unmappedFieldAfter_loadedAsKeyword() {
        // load() is orthogonal to ENRICH — downstream unmapped fields still get PotentiallyUnmappedKeyword treatment.
        var plan = testWithLanguagesEnrich().statement(
            setUnmappedLoad("FROM test | EVAL language_code = languages | ENRICH languages ON language_code | EVAL x = does_not_exist")
        );
        var x = singleValue(plan.output().stream().filter(a -> "x".equals(a.name())).toList());
        assertThat(x.dataType(), is(DataType.KEYWORD));
    }

    public void testLoad_unmappedKey_loadedAsKeyword() {
        // load() promotes absent key to KEYWORD; MATCH-type enrich policy accepts keyword fields.
        var plan = testWithLanguagesEnrich().statement(
            setUnmappedLoad("FROM test | ENRICH languages ON language_code | KEEP emp_no, language_name")
        );
        var languageName = singleValue(plan.output().stream().filter(a -> "language_name".equals(a.name())).toList());
        assertThat(languageName.dataType(), is(DataType.KEYWORD));
    }

    public void testNullify_allMapped_doesNotThrow() {
        // Baseline: all fields mapped, ENRICH works normally under nullify mode.
        testWithLanguagesEnrich().statement(
            setUnmappedNullify(
                "FROM test | EVAL language_code = languages | ENRICH languages ON language_code | KEEP emp_no, language_name"
            )
        );
    }

    public void testNullify_unmappedKey_nullifiedMatchField_loadedAsKeyword() {
        // nullify() sets key type to NULL; resolveEnrich() skips type-validation for NULL-typed keys.
        // Analysis succeeds, but the key is always null at runtime → ENRICH produces no matches.
        var plan = testWithLanguagesEnrich().statement(
            setUnmappedNullify("FROM test | ENRICH languages ON language_code | KEEP emp_no, language_name")
        );
        var languageName = singleValue(plan.output().stream().filter(a -> "language_name".equals(a.name())).toList());
        assertThat(languageName.dataType(), is(DataType.KEYWORD));
    }
}
