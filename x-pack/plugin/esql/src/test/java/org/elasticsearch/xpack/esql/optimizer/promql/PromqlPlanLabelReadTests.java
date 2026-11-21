/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.CsvSpecReader;
import org.elasticsearch.xpack.esql.SpecReader;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.AbstractTimeSeriesMetadataFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataPath;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.ScalarizeTimeSeriesMetadataPaths;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.classpathResources;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Plans every {@code PROMQL} query over {@code k8s} of the csv-spec files and checks what
 * {@link ScalarizeTimeSeriesMetadataPaths} leaves of the reads of labels out of the label container: a read costs a parse of
 * the container per row, a column next to nothing.
 * <ul>
 *     <li>No read of a {@code keyword} dimension is left: the plan always has its column.</li>
 *     <li>The rule changes nothing it already rewrote.</li>
 * </ul>
 * A read of any other label stays: the container is loaded from the source, so a {@code long} dimension written as {@code 1.0}
 * reads as text that no column holds.
 */
public class PromqlPlanLabelReadTests extends AbstractPromqlPlanOptimizerTests {

    public PromqlPlanLabelReadTests(VersionMode versionMode) {
        super(versionMode);
    }

    @Override
    protected boolean enableWarningsCheck() {
        return false;
    }

    public void testSpecQueriesReadNoKeywordDimension() throws Exception {
        assumeTrue("the label container functions", containerFunctions());
        List<URL> urls = classpathResources("/*.csv-spec");
        List<String> failures = new ArrayList<>();
        int planned = 0;
        for (Object[] spec : SpecReader.readScriptSpec(urls, CsvSpecReader::specParser)) {
            CsvSpecReader.CsvTestCase testCase = (CsvSpecReader.CsvTestCase) spec[4];
            // The k8s index is the one the test analyzer holds; the prometheus-k8s specs repeat its queries.
            if (testCase.query.contains("PROMQL index=k8s ") == false) {
                continue;
            }
            LogicalPlan plan;
            try {
                plan = tsAnalyzer().query(
                    testCase.query.replace("$now-1h", "\"2024-05-10T00:00:00Z\"").replace("$now", "\"2024-05-10T01:00:00Z\"")
                );
            } catch (VerificationException | ParsingException e) {
                // Queries a spec expects to fail.
                continue;
            } catch (RuntimeException e) {
                throw new AssertionError(spec[0] + ":" + spec[2], e);
            }
            List<TimeSeriesMetadataPath> reads = new ArrayList<>();
            plan.forEachExpressionDown(TimeSeriesMetadataPath.class, reads::add);
            Set<String> dimensions = keywordDimensions(plan);
            for (TimeSeriesMetadataPath read : reads) {
                if (read.children().get(1) instanceof Literal name
                    && name.value() instanceof BytesRef label
                    && dimensions.contains(label.utf8ToString())) {
                    failures.add(spec[0] + ":" + spec[2] + " reads [" + label.utf8ToString() + "]");
                }
            }
            if (new ScalarizeTimeSeriesMetadataPaths().apply(plan).equals(plan) == false) {
                failures.add(spec[0] + ":" + spec[2] + " changes when scalarized again");
            }
            if (plan.anyMatch(
                p -> p.expressions().stream().anyMatch(e -> e.anyMatch(AbstractTimeSeriesMetadataFunction.class::isInstance))
            )) {
                planned++;
            }
        }
        assertThat(String.join("\n", failures), failures, empty());
        assertThat("queries planned with the label container", planned, greaterThan(0));
    }

    private static Set<String> keywordDimensions(LogicalPlan plan) {
        Set<String> dimensions = new HashSet<>();
        for (EsRelation relation : plan.collect(EsRelation.class)) {
            for (var field : relation.output()) {
                if (field instanceof FieldAttribute dimension && dimension.isDimension() && dimension.dataType() == DataType.KEYWORD) {
                    dimensions.add(PromqlLabels.labelName(dimension));
                }
            }
        }
        return dimensions;
    }
}
