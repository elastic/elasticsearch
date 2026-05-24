/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.session.Versioned;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_SEARCH_STATS;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;

/**
 * Helper for testing all things related to {@link LogicalPlanOptimizer#optimize}
 * and {@link LocalLogicalPlanOptimizer#localOptimize}. Defaults an empty config.
 */
public class TestOptimizer {
    private final TestAnalyzer analyzer = new TestAnalyzer();
    private SearchStats searchStats = TEST_SEARCH_STATS;

    /**
     * Set the {@link SearchStats} used for optimization.
     */
    public TestOptimizer searchStats(SearchStats searchStats) {
        this.searchStats = searchStats;
        return this;
    }

    private LogicalPlanOptimizer buildLogicalOptimizer() {
        return new LogicalPlanOptimizer(unboundLogicalOptimizerContext());
    }

    private LocalLogicalPlanOptimizer buildLocalLogicalOptimizer() {
        var localContext = new LocalLogicalOptimizerContext(analyzer.configuration(), FoldContext.small(), searchStats);
        return new LocalLogicalPlanOptimizer(localContext);
    }

    /**
     * Builds the coordinating node {@link LogicalPlan}. The steps:
     * <ol>
     *     <li>Parse</li>
     *     <li>Analyze</li>
     *     <li>Run coordinating node logical plan optimizations</li>
     * </ol>
     */
    public LogicalPlan coordinatorPlan(String query) {
        return buildLogicalOptimizer().optimize(analyzer.query(query));
    }

    /**
     * Builds the local node {@link LogicalPlan}. The steps:
     * <ol>
     *     <li>Parse</li>
     *     <li>Analyze</li>
     *     <li>Run coordinating node logical plan optimizations</li>
     *     <li>Run local node logical plan optimizations</li>
     * </ol>
     */
    public LogicalPlan localPlan(String query) {
        return buildLocalLogicalOptimizer().localOptimize(coordinatorPlan(query));
    }

    /**
     * A {@link LogicalPlan} and {@link PhysicalPlan}.
     */
    public record LogicalAndPhysical(LogicalPlan logical, PhysicalPlan physical) {}

    /**
     * Builds the {@link PhysicalPlan}. The steps:
     * <ol>
     *     <li>Parse</li>
     *     <li>Analyze</li>
     *     <li>Run coordinating node logical plan optimizations</li>
     *     <li>Run local node logical plan optimizations</li>
     *     <li>Run the physical plan {@link Mapper}</li>
     * </ol>
     */
    public LogicalAndPhysical physicalPlan(String query) {
        Analyzer analyzer = this.analyzer.buildAnalyzer();
        LogicalPlan logical = buildLogicalOptimizer().optimize(analyzer.analyze(TEST_PARSER.parseQuery(query)));
        logical = buildLocalLogicalOptimizer().localOptimize(logical);
        PhysicalPlan physical = new Mapper().map(new Versioned<>(logical, analyzer.context().minimumVersion()));
        return new LogicalAndPhysical(logical, physical);
    }

    // =======================================
    // Below this is delegates to TestAnalyzer
    // =======================================

    /**
     * Adds an index resolution by loading the mapping from a resource file.
     */
    public TestOptimizer addIndex(String name, String mappingFile) {
        analyzer.addIndex(name, mappingFile);
        return this;
    }

    /**
     * Adds an index resolution by loading the mapping from a resource file.
     */
    public TestOptimizer addIndex(String name, String mappingFile, IndexMode indexMode) {
        analyzer.addIndex(name, mappingFile, indexMode);
        return this;
    }

    /**
     * Add an index to the query.
     */
    public TestOptimizer addIndex(EsIndex index) {
        analyzer.addIndex(index);
        return this;
    }

    /**
     * Adds a lookup index by loading the mapping from a resource file.
     */
    public TestOptimizer addLookupIndex(String name, String mappingLocation) {
        analyzer.addLookupIndex(name, mappingLocation);
        return this;
    }

    /**
     * Adds the languages index.
     */
    public TestOptimizer addLanguages() {
        analyzer.addLanguages();
        return this;
    }

    /**
     * Adds the languages_lookup lookup index.
     */
    public TestOptimizer addLanguagesLookup() {
        analyzer.addLanguagesLookup();
        return this;
    }

    /**
     * Adds the sample_data index.
     */
    public TestOptimizer addSampleData() {
        analyzer.addSampleData();
        return this;
    }

    /**
     * Adds the sample_data_lookup lookup index.
     */
    public TestOptimizer addSampleDataLookup() {
        analyzer.addSampleDataLookup();
        return this;
    }

    /**
     * Adds the test_lookup lookup index.
     */
    public TestOptimizer addTestLookup() {
        analyzer.addTestLookup();
        return this;
    }

    /**
     * Adds the spatial_lookup lookup index.
     */
    public TestOptimizer addSpatialLookup() {
        analyzer.addSpatialLookup();
        return this;
    }

    /**
     * Adds the test index with mapping-default.json.
     */
    public TestOptimizer addDefaultIndex() {
        analyzer.addDefaultIndex();
        return this;
    }

    /**
     * Adds the airports index.
     */
    public TestOptimizer addAirports() {
        analyzer.addAirports();
        return this;
    }

    /**
     * Adds the test_mixed_types index with mapping-default-incompatible.json.
     */
    public TestOptimizer addDefaultIncompatible() {
        analyzer.addDefaultIncompatible();
        return this;
    }

    /**
     * Adds the k8s index with k8s-mappings.json in time series mode.
     */
    public TestOptimizer addK8s() {
        analyzer.addK8s();
        return this;
    }

    /**
     * Adds the k8s index with k8s-downsampled-mappings.json in time series mode.
     */
    public TestOptimizer addK8sDownsampled() {
        analyzer.addK8sDownsampled();
        return this;
    }

    /**
     * Adds an enrich policy resolution by loading the mapping from a resource file.
     */
    public TestOptimizer addEnrichPolicy(String policyType, String policy, String field, String index, String mapping) {
        analyzer.addEnrichPolicy(policyType, policy, field, index, mapping);
        return this;
    }
}
