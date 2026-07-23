/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.LowercaseNormalizer;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper.KeywordFieldType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.test.IndexSettingsModule;
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
import java.util.Map;

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

    public void testPotentiallyUnmappedFieldNormalizedFromSearchStats() {
        var plan = planWithLoad("""
              FROM test
            | WHERE does_not_exist == "x"
            | KEEP does_not_exist
            """);

        // A normalizer rewrites indexed values, so the replacement must carry normalized=true (which disables exact-match pushdown).
        var normalizedOnDataNode = new TestConfigurableSearchStats() {
            @Override
            public MappedFieldType fieldType(FieldAttribute.FieldName name) {
                return name.string().equals("does_not_exist") ? normalizedKeywordFieldType(name.string()) : super.fieldType(name);
            }
        };

        var localFields = fieldAttributes(localPlan(plan, normalizedOnDataNode), "does_not_exist");
        assertThat(localFields, not(empty()));
        for (FieldAttribute f : localFields) {
            assertThat(f.field().getClass(), equalTo(KeywordEsField.class));
            assertThat(((KeywordEsField) f.field()).getNormalized(), equalTo(true));
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

    private static KeywordFieldType normalizedKeywordFieldType(String name) {
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);
        var builder = new KeywordFieldMapper.Builder(name, IndexAnalyzers.of(Map.of()), ScriptCompiler.NONE, indexSettings, false, false);
        var normalizer = new NamedAnalyzer("lowercase", AnalyzerScope.INDEX, new LowercaseNormalizer());
        return new KeywordFieldType(name, IndexType.terms(true, true), TextSearchInfo.SIMPLE_MATCH_ONLY, normalizer, builder, false);
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
