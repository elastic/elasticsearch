/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.xpack.esql.EsqlTestUtils.TestConfigurableSearchStats;
import org.elasticsearch.xpack.esql.EsqlTestUtils.TestConfigurableSearchStats.Config;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.optimizer.AbstractLocalLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class ReplacePotentiallyUnmappedFieldWithMappedFieldTests extends AbstractLocalLogicalPlanOptimizerTests {

    public void testPotentiallyUnmappedFieldReplacedWhenIndexedOnDataNode() {
        assertLoadModeFieldReplacedWithPushableKeyword(new TestConfigurableSearchStats());
    }

    public void testPotentiallyUnmappedFieldReplacedWhenOnlyDocValuesOnDataNode() {
        assertLoadModeFieldReplacedWithPushableKeyword(new TestConfigurableSearchStats().exclude(Config.INDEXED, "does_not_exist"));
    }

    private void assertLoadModeFieldReplacedWithPushableKeyword(TestConfigurableSearchStats localShardStats) {
        var plan = planWithLoad("""
              FROM test
            | WHERE does_not_exist == "x"
            | KEEP does_not_exist
            """);

        // The coordinator can't know the field is mapped on any given data node, so it stays potentially unmapped there.
        var coordinatorFields = fieldAttributes(plan, "does_not_exist");
        assertThat(coordinatorFields, not(empty()));
        for (FieldAttribute f : coordinatorFields) {
            assertThat(f.field(), instanceOf(PotentiallyUnmappedKeywordEsField.class));
        }

        var pushdown = LucenePushdownPredicates.from(localShardStats, new EsqlFlags(true));
        var localFields = fieldAttributes(localPlan(plan, localShardStats), "does_not_exist");
        assertThat(localFields, not(empty()));
        for (FieldAttribute f : localFields) {
            assertThat(f.field().getClass(), equalTo(KeywordEsField.class));
            assertThat(f.dataType(), equalTo(KEYWORD));
            assertTrue(pushdown.isPushableFieldAttribute(f));
        }
    }

    public void testPotentiallyUnmappedFieldRetainedWhenOnlyInSourceOnDataNode() {
        var plan = planWithLoad("""
              FROM test
            | WHERE does_not_exist == "x"
            | KEEP does_not_exist
            """);

        var fieldOnlyInSourceOnLocalShards = new TestConfigurableSearchStats().exclude(Config.INDEXED, "does_not_exist")
            .exclude(Config.DOC_VALUES, "does_not_exist");

        var pushdown = LucenePushdownPredicates.from(fieldOnlyInSourceOnLocalShards, new EsqlFlags(true));
        var localFields = fieldAttributes(localPlan(plan, fieldOnlyInSourceOnLocalShards), "does_not_exist");
        assertThat(localFields, not(empty()));
        for (FieldAttribute f : localFields) {
            assertThat(f.field(), instanceOf(PotentiallyUnmappedKeywordEsField.class));
            // Neither indexed nor doc-valued here, so it can't be read as a mapped field and stays loaded from _source (also unpushable).
            assertFalse(pushdown.isPushableFieldAttribute(f));
        }
    }

    public void testLookupIndexFieldsNotModified() {
        Analyzer analyzer = analyzer().unmappedResolution(UnmappedResolution.LOAD)
            .addIndex("test", "mapping-basic.json")
            .addLanguagesLookup()
            .buildAnalyzer();
        var plan = optimize(analyzer.analyze(TEST_PARSER.parseQuery("""
              FROM test
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE does_not_exist == "x"
            | KEEP language_name, does_not_exist
            """)));

        var localPlan = localPlan(plan, new TestConfigurableSearchStats());

        // language_name comes from the lookup index, never as a potentially-unmapped marker, so the rule must leave it alone.
        var lookupFields = fieldAttributes(localPlan, "language_name");
        assertThat(lookupFields, not(empty()));
        for (FieldAttribute f : lookupFields) {
            assertThat(f.field().getClass(), equalTo(KeywordEsField.class));
        }

        // Sanity anchor: the main-index unmapped field still gets replaced, so this would catch an over-broad rule.
        var mainFields = fieldAttributes(localPlan, "does_not_exist");
        assertThat(mainFields, not(empty()));
        for (FieldAttribute f : mainFields) {
            assertThat(f.field().getClass(), equalTo(KeywordEsField.class));
        }
    }

    private LogicalPlan planWithLoad(String query) {
        Analyzer analyzer = analyzer().unmappedResolution(UnmappedResolution.LOAD).addIndex("test", "mapping-basic.json").buildAnalyzer();
        return optimize(analyzer.analyze(TEST_PARSER.parseQuery(query)));
    }

    private static List<FieldAttribute> fieldAttributes(LogicalPlan plan, String name) {
        List<FieldAttribute> matches = new ArrayList<>();
        plan.forEachExpressionDown(FieldAttribute.class, f -> {
            if (f.name().equals(name)) {
                matches.add(f);
            }
        });
        return matches;
    }
}
