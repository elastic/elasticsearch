/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.FileSplit;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.util.List;
import java.util.Map;

public class ExternalDistributionTests extends ESTestCase {

    private static final Source SRC = Source.EMPTY;

    // --- Mapper tests ---

    public void testMapperCreatesSingleAggForExternalSource() {
        ExternalRelation external = createExternalRelation();
        List<Expression> groupings = List.of();
        List<? extends NamedExpression> aggregates = List.of();
        Aggregate aggregate = new Aggregate(SRC, external, groupings, aggregates);

        Mapper mapper = new Mapper();
        PhysicalPlan physicalPlan = mapper.map(new Versioned<>(aggregate, TransportVersion.current()));

        assertTrue("Expected AggregateExec at top, got: " + physicalPlan.getClass().getSimpleName(), physicalPlan instanceof AggregateExec);
        AggregateExec aggExec = (AggregateExec) physicalPlan;
        assertEquals(AggregatorMode.SINGLE, aggExec.getMode());
        assertTrue(
            "Expected ExternalSourceExec child, got: " + aggExec.child().getClass().getSimpleName(),
            aggExec.child() instanceof ExternalSourceExec
        );
    }

    public void testMapperDoesNotInsertExchangeForLimitAboveExternalSource() {
        ExternalRelation external = createExternalRelation();
        Limit limit = new Limit(SRC, new Literal(SRC, 10, DataType.INTEGER), external);

        Mapper mapper = new Mapper();
        PhysicalPlan physicalPlan = mapper.map(new Versioned<>(limit, TransportVersion.current()));

        assertTrue("Expected LimitExec at top, got: " + physicalPlan.getClass().getSimpleName(), physicalPlan instanceof LimitExec);
        LimitExec limitExec = (LimitExec) physicalPlan;
        assertTrue(
            "Expected ExternalSourceExec child (no exchange), got: " + limitExec.child().getClass().getSimpleName(),
            limitExec.child() instanceof ExternalSourceExec
        );
    }

    // --- Context validation tests ---

    public void testContextRejectsNullSplits() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new ExternalDistributionContext(createExternalSourceExec(), null, null, QueryPragmas.EMPTY)
        );
    }

    public void testContextRejectsNullPlan() {
        expectThrows(IllegalArgumentException.class, () -> new ExternalDistributionContext(null, List.of(), null, QueryPragmas.EMPTY));
    }

    // --- Strategy resolution tests ---

    public void testResolveStrategyAdaptiveDefault() {
        ExternalDistributionStrategy strategy = ComputeService.resolveExternalDistributionStrategy(QueryPragmas.EMPTY);
        assertTrue(strategy instanceof AdaptiveStrategy);
    }

    public void testResolveStrategyCoordinatorOnly() {
        Settings settings = Settings.builder().put("external_distribution", "coordinator_only").build();
        QueryPragmas pragmas = new QueryPragmas(settings);
        ExternalDistributionStrategy strategy = ComputeService.resolveExternalDistributionStrategy(pragmas);
        assertTrue(strategy instanceof CoordinatorOnlyStrategy);
    }

    public void testResolveStrategyRoundRobin() {
        Settings settings = Settings.builder().put("external_distribution", "round_robin").build();
        QueryPragmas pragmas = new QueryPragmas(settings);
        ExternalDistributionStrategy strategy = ComputeService.resolveExternalDistributionStrategy(pragmas);
        assertTrue(strategy instanceof RoundRobinStrategy);
    }

    // --- Exchange collapsing tests (safety net for PR5) ---

    public void testCollapseExternalSourceExchangeRemovesExchange() {
        ExternalSourceExec externalSource = createExternalSourceExec();
        ExchangeExec exchange = new ExchangeExec(SRC, externalSource);
        LimitExec limit = new LimitExec(SRC, exchange, new Literal(SRC, 10, DataType.INTEGER), null);

        PhysicalPlan collapsed = ComputeService.collapseExternalSourceExchanges(limit);

        assertTrue("Expected LimitExec at top", collapsed instanceof LimitExec);
        LimitExec collapsedLimit = (LimitExec) collapsed;
        assertTrue(
            "Expected ExternalSourceExec directly under LimitExec, got: " + collapsedLimit.child().getClass().getSimpleName(),
            collapsedLimit.child() instanceof ExternalSourceExec
        );
    }

    public void testCollapseExternalSourceExchangePreservesNonExternalExchanges() {
        ExternalSourceExec externalSource = createExternalSourceExec();
        AggregateExec agg = new AggregateExec(SRC, externalSource, List.of(), List.of(), AggregatorMode.INITIAL, List.of(), null);
        ExchangeExec exchange = new ExchangeExec(SRC, agg);

        PhysicalPlan collapsed = ComputeService.collapseExternalSourceExchanges(exchange);

        assertTrue("Exchange wrapping non-ExternalSourceExec should be preserved", collapsed instanceof ExchangeExec);
    }

    public void testCollapseDoesNothingWhenNoExchanges() {
        ExternalSourceExec externalSource = createExternalSourceExec();
        LimitExec limit = new LimitExec(SRC, externalSource, new Literal(SRC, 10, DataType.INTEGER), null);

        PhysicalPlan collapsed = ComputeService.collapseExternalSourceExchanges(limit);

        assertTrue(collapsed instanceof LimitExec);
        LimitExec collapsedLimit = (LimitExec) collapsed;
        assertTrue(collapsedLimit.child() instanceof ExternalSourceExec);
    }

    public void testCollapseWithSplitsOnExternalSource() {
        List<ExternalSplit> splits = List.of(
            new FileSplit("parquet", StoragePath.of("s3://bucket/file1.parquet"), 0, 1024, ".parquet", Map.of(), Map.of()),
            new FileSplit("parquet", StoragePath.of("s3://bucket/file2.parquet"), 0, 1024, ".parquet", Map.of(), Map.of())
        );
        ExternalSourceExec externalSource = createExternalSourceExec().withSplits(splits);
        ExchangeExec exchange = new ExchangeExec(SRC, externalSource);
        LimitExec limit = new LimitExec(SRC, exchange, new Literal(SRC, 10, DataType.INTEGER), null);

        PhysicalPlan collapsed = ComputeService.collapseExternalSourceExchanges(limit);

        assertTrue(collapsed instanceof LimitExec);
        LimitExec collapsedLimit = (LimitExec) collapsed;
        assertTrue(collapsedLimit.child() instanceof ExternalSourceExec);
        ExternalSourceExec collapsedSource = (ExternalSourceExec) collapsedLimit.child();
        assertEquals(2, collapsedSource.splits().size());
    }

    // --- Helpers ---

    private static ExternalRelation createExternalRelation() {
        List<Attribute> output = List.of(
            new FieldAttribute(SRC, "name", new EsField("name", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE))
        );
        SimpleSourceMetadata metadata = new SimpleSourceMetadata(output, "parquet", "s3://bucket/data/*.parquet");
        return new ExternalRelation(SRC, "s3://bucket/data/*.parquet", metadata, output, null);
    }

    private static ExternalSourceExec createExternalSourceExec() {
        return new ExternalSourceExec(
            SRC,
            "s3://bucket/data/*.parquet",
            "parquet",
            List.of(
                new FieldAttribute(SRC, "name", new EsField("name", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE))
            ),
            Map.of(),
            Map.of(),
            null
        );
    }
}
