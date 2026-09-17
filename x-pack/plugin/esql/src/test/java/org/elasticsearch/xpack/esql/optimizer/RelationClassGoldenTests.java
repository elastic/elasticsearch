/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.cluster.metadata.DataSourceReference;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolution;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;

/**
 * Plan snapshots for {@code _class} and {@code _name}: what the rule that answers them actually leaves in a plan.
 *
 * <p>Two things these pin that prose cannot. First, the shape — both columns leave the relation's output entirely and
 * come back as an {@code Eval} of a literal (for {@code _class}, and for a dataset's {@code _name}) or of an alias over
 * {@code _index} (for an index's {@code _name}), so no data source is ever asked for a column it cannot answer.
 *
 * <p>Second, the cost, which these snapshots measure rather than predict. {@code PushDownUtils.isLeafUnionAll} accepts
 * only a bare relation or a {@code Project} directly over one, so the {@code Eval} makes a {@code UnionAll} non-leaf.
 * {@link #testHeavyAggregateWithClassRequested} differs from {@code HeterogeneousFromPushdownGoldenTests.testCountDistinctPushed}
 * only by {@code METADATA _class}, and its plan keeps the aggregate above the union where the other decomposes it into a
 * {@code TOPARTIAL} per branch. Asking for the column costs that decomposition.
 */
public class RelationClassGoldenTests extends GoldenTestCase {

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public RelationClassGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.LOGICAL_OPTIMIZATION, Stage.PHYSICAL_OPTIMIZATION);

    private static final String RESOURCE_A = "s3://bucket/class_a.parquet";
    private static final String RESOURCE_B = "s3://bucket/class_b.parquet";

    /** A dataset answers both columns as literals: it has one kind and one name. */
    public void testBothColumnsOnASingleDataset() {
        runGoldenTest("FROM class_a METADATA _class, _name | KEEP emp_no, _class, _name");
    }

    /** Each branch of a union answers for itself, so the literals differ per branch rather than folding once. */
    public void testBothColumnsAcrossAUnion() {
        runGoldenTest("FROM class_a, class_b METADATA _class, _name | KEEP emp_no, _class, _name");
    }

    /**
     * The pushdown question, stated as a pair. Without {@code METADATA _class} this is
     * {@code HeterogeneousFromPushdownGoldenTests.testCountDistinctPushed}, where the heavy aggregate decomposes into
     * each branch. The snapshot says what the added {@code Eval} does to that.
     */
    public void testHeavyAggregateWithClassRequested() {
        runGoldenTest("FROM class_a, class_b METADATA _class | STATS d = COUNT_DISTINCT(emp_no)");
    }

    /** Grouping on _class is the shape a user reaches for the column with; it also forces it past the aggregate. */
    public void testGroupingOnClass() {
        runGoldenTest("FROM class_a, class_b METADATA _class | STATS d = COUNT_DISTINCT(emp_no) BY _class");
    }

    private void runGoldenTest(String query) {
        assumeTrue("Requires external data source FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
        builder(query).stages(STAGES).datasetMetadata(datasetMetadata()).externalSourceResolution(externalSourceResolution()).run();
    }

    private static ProjectMetadata datasetMetadata() {
        DataSource dataSource = new DataSource("class_ds", "test", null, Map.of());
        Dataset a = new Dataset("class_a", new DataSourceReference("class_ds"), RESOURCE_A, null, Map.of());
        Dataset b = new Dataset("class_b", new DataSourceReference("class_ds"), RESOURCE_B, null, Map.of());
        return ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(Map.of("class_ds", dataSource)))
            .datasets(Map.of("class_a", a, "class_b", b))
            .build();
    }

    /** Identical schemas on both branches, so the UnionAll is leaf-shaped until this rule touches it. */
    private static ExternalSourceResolution externalSourceResolution() {
        return new ExternalSourceResolution(
            Map.of(
                RESOURCE_A,
                new ExternalSourceResolution.ResolvedSource(schemaFor(RESOURCE_A), FileList.UNRESOLVED, Map.of()),
                RESOURCE_B,
                new ExternalSourceResolution.ResolvedSource(schemaFor(RESOURCE_B), FileList.UNRESOLVED, Map.of())
            )
        );
    }

    private static ExternalSourceMetadata schemaFor(String resource) {
        List<Attribute> schema = List.of(
            referenceAttribute("emp_no", DataType.INTEGER),
            referenceAttribute("salary", DataType.INTEGER),
            referenceAttribute("dept", DataType.INTEGER)
        );
        return new ExternalSourceMetadata() {
            @Override
            public String location() {
                return resource;
            }

            @Override
            public List<Attribute> schema() {
                return schema;
            }

            @Override
            public String sourceType() {
                return "parquet";
            }
        };
    }
}
