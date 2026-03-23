/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.enrich.ResolvedEnrichPolicy;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.planner.FilterTests;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;

public class AbstractLocalPhysicalPlanOptimizerTests extends MapperServiceTestCase {
    protected final Configuration config;

    private static final String PARAM_FORMATTING = "%1$s";

    @ParametersFactory(argumentFormatting = PARAM_FORMATTING)
    public static List<Object[]> readScriptSpec() {
        return settings().stream().map(t -> {
            var settings = Settings.builder().loadFromMap(t.v2()).build();
            return new Object[] { t.v1(), configuration(new QueryPragmas(settings)) };
        }).toList();
    }

    private static List<Tuple<String, Map<String, Object>>> settings() {
        return List.of(new Tuple<>("default", Map.of()));
    }

    protected static QueryBuilder wrapWithSingleQuery(String query, QueryBuilder inner, String fieldName, Source source) {
        return FilterTests.singleValueQuery(query, inner, fieldName, source);
    }

    public AbstractLocalPhysicalPlanOptimizerTests(String name, Configuration config) {
        this.config = config;
    }

    protected TestAnalyzer defaultAnalyzer() {
        return analyzer().configuration(config)
            .addEmployees("test")
            .addTestLookup()
            .addLanguagesLookup()
            .addSpatialLookup()
            .addEnrichPolicy(Enrich.Mode.ANY, "foo", fooPolicy());
    }

    protected TestAnalyzer timeSeries() {
        return analyzer().addK8s().addEnrichPolicy(Enrich.Mode.ANY, "foo", fooPolicy());
    }

    private ResolvedEnrichPolicy fooPolicy() {
        return new ResolvedEnrichPolicy(
            "fld",
            EnrichPolicy.MATCH_TYPE,
            List.of("a", "b"),
            Map.of("", "idx"),
            Map.ofEntries(
                Map.entry("a", new EsField("a", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE)),
                Map.entry("b", new EsField("b", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.NONE))
            )
        );
    }

    /**
     * This exempts the warning about adding the automatic limit from the warnings check in
     * {@link ESTestCase#ensureNoWarnings()}
     */
    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    protected TestAnalyzer allTypes() {
        return analyzer().configuration(config).addIndex("test", "mapping-all-types.json");
    }
}
