/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VersionMode;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.into.IntoOptions;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Into;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class IntoAnalysisTests extends AnalyzerTestCase {
    public IntoAnalysisTests(VersionMode versionMode) {
        super(versionMode);
    }

    @Before
    public void requiresSnapshot() {
        assumeTrue("INTO is only enabled in snapshot builds", EsqlCapabilities.Cap.INTO_COMMAND.isEnabled());
    }

    public void testNoImplicitLimitAdded() {
        for (String query : List.of("ROW a = 1 | INTO dst", "FROM employees | KEEP emp_no | INTO dst")) {
            Into into = as(analyzer().addEmployees().query(query), Into.class);
            assertThat(into.collectFirstChildren(Limit.class::isInstance), empty());
        }
    }

    public void testDescribeAfterIntoIsRejected() {
        analyzer().error("ROW a = 1 | INTO dst | DESCRIBE", containsString("INTO must be the last command of the query"));
    }

    public void testSyntheticAttributesStrippedFromChildOutput() {
        LogicalPlan plan = analyzer().query("ROW a = 1 | WHERE a IN (ROW b = 1) OR a > 0 | INTO dst");
        Into into = as(plan, Into.class);
        long syntheticCount = into.child()
            .output()
            .stream()
            .filter(attr -> attr.name().startsWith(Attribute.SYNTHETIC_ATTRIBUTE_NAME_PREFIX))
            .count();
        assertThat(syntheticCount, equalTo(0L));
    }

    public void testMustBeTheLastTopLevelCommand() {
        String error = "INTO must be the last command of the query";
        analyzer().stripErrorPrefix(true).error("ROW a = 1 | INTO dst | LIMIT 1", equalTo("1:13: " + error));
        analyzer().error("ROW a = 1 | FORK (INTO dst) (LIMIT 1)", containsString(error));
        analyzer().error("ROW a = 1 | FORK (INTO dst WITH {\"keep_going\": true}) (LIMIT 1)", containsString(error));
    }

    public void testUnsupportedSources() {
        String error = "INTO currently only supports ROW or local FROM as a source, without FORK";
        analyzer().addK8s().error("TS k8s | INTO dst", containsString(error));
        analyzer().addIndex("remote:employees", "mapping-default.json").error("FROM remote:employees | INTO dst", containsString(error));
        analyzer().error("ROW a = 1 | FORK (WHERE true) (WHERE false) | INTO dst", containsString(error));
    }

    public void testSortWithLimitIsAnEligibleSource() {
        Into into = as(analyzer().addEmployees().query("FROM employees | SORT emp_no | LIMIT 10 | INTO dst"), Into.class);
        Limit limit = as(into.child(), Limit.class);
        as(limit.child(), OrderBy.class);
        assertThat(into.collect(Limit.class), hasSize(1));
    }

    public void testUnboundedSortBeforeIntoIsRejected() {
        String error = "INTO cannot be preceded by an unbounded SORT [SORT emp_no]";
        analyzer().addEmployees().error("FROM employees | SORT emp_no | INTO dst", containsString(error));
        analyzer().addEmployees().error("FROM employees | SORT emp_no | KEEP emp_no | INTO dst", containsString(error));
        analyzer().addEmployees().error("FROM employees | SORT emp_no | LIMIT 5 | SORT emp_no | INTO dst", containsString(error));
        analyzer().error("ROW a = 1 | SORT a | INTO dst", containsString("INTO cannot be preceded by an unbounded SORT [SORT a]"));
    }

    public void testSortThatAnAggregationMakesRedundantIsAnEligibleSource() {
        as(analyzer().addEmployees().query("FROM employees | SORT emp_no | STATS c = COUNT(*) BY gender | INTO dst"), Into.class);
    }

    public void testUnsupportedSourcesBelowDescribeAreRejected() {
        String error = Into.UNSUPPORTED_SOURCE;
        analyzer().error("ROW a = 1 | FORK (WHERE true) (WHERE false) | DESCRIBE | INTO dst", containsString(error));
        analyzer().addK8s().error("TS k8s | DESCRIBE | INTO dst", containsString(error));
        analyzer().addIndex("remote:employees", "mapping-default.json")
            .error("FROM remote:employees | DESCRIBE | INTO dst", containsString(error));
    }

    public void testDescribeIsAnEligibleSource() {
        List<String> queries = List.of(
            "ROW a = 1 | DESCRIBE | INTO dst",
            "FROM employees | DESCRIBE | INTO dst",
            "FROM employees | KEEP emp_no, salary | DESCRIBE | INTO dst"
        );
        for (String query : queries) {
            Into into = as(analyzer().addEmployees().query(query), Into.class);
            assertThat(query, into.child().anyMatch(LocalRelation.class::isInstance), equalTo(true));
            assertThat(query, into.child().anyMatch(p -> p instanceof Row || p instanceof EsRelation), equalTo(false));
        }
    }

    public void testIdColumnMayBeAString() {
        analyzer().query("ROW _id = \"a\", x = 1 | INTO dst");
        analyzer().query("ROW _id = null, x = 1 | INTO dst");
        analyzer().addEmployees().query("FROM employees | EVAL _id = emp_no::keyword | INTO dst");
        analyzer().addEmployees().query("FROM employees METADATA _id | INTO dst");
    }

    public void testIdColumnOfAnotherTypeIsRejected() {
        String error = "INTO column [_id] must be of type [keyword] or [text], found [integer]";
        analyzer().error("ROW _id = 1, x = 1 | INTO dst", containsString(error));
        analyzer().addEmployees().error("FROM employees | EVAL _id = emp_no | KEEP _id | INTO dst", containsString(error));
        analyzer().error("ROW _id = true | INTO dst", containsString("must be of type [keyword] or [text], found [boolean]"));
    }

    public void testReplaceIsRejectedOnDataStreams() {
        TestAnalyzer analyzer = analyzer().projectMetadata(projectWithDataStream());
        for (String destination : List.of("logs-ds", "logs-ds-alias", BACKING_INDEX, "auto-ds-new")) {
            analyzer.error(
                "ROW a = 1 | INTO \"" + destination + "\" WITH {\"collisions\": \"replace\"}",
                containsString("INTO collisions [replace] is not supported on data stream [" + destination + "]")
            );
        }
    }

    public void testReplaceIsAllowedOnIndices() {
        TestAnalyzer analyzer = analyzer().projectMetadata(projectWithDataStream());
        for (String destination : List.of("plain", "fresh")) {
            Into into = as(analyzer.query("ROW a = 1 | INTO " + destination + " WITH {\"collisions\": \"replace\"}"), Into.class);
            assertThat(into.options().collisions(), equalTo(IntoOptions.Collisions.REPLACE));
        }
    }

    public void testKeepAndAbortAreAllowedOnDataStreams() {
        TestAnalyzer analyzer = analyzer().projectMetadata(projectWithDataStream());
        for (String collisions : List.of("keep", "abort")) {
            analyzer.query("ROW a = 1 | INTO logs-ds WITH {\"collisions\": \"" + collisions + "\"}");
            analyzer.query("ROW a = 1 | INTO auto-ds-new WITH {\"collisions\": \"" + collisions + "\"}");
        }
    }

    private static final String BACKING_INDEX = ".ds-logs-ds-2026.01.01-000001";

    /**
     * A data stream {@code logs-ds} with one backing index and an alias {@code logs-ds-alias}, a regular index {@code plain},
     * and a data stream template matching {@code auto-ds-*}, so writing to a missing {@code auto-ds-*} creates a data stream.
     */
    private static ProjectMetadata projectWithDataStream() {
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        IndexMetadata backing = IndexMetadata.builder(BACKING_INDEX)
            .settings(settings(IndexVersion.current()).put(IndexMetadata.SETTING_INDEX_HIDDEN, true))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        builder.put(backing, false);
        builder.put(DataStreamTestHelper.newInstance("logs-ds", List.of(backing.getIndex())));
        builder.put("logs-ds-alias", "logs-ds", true, null);
        builder.put(IndexMetadata.builder("plain").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(0));
        builder.put(
            "auto-ds-template",
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of("auto-ds-*"))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
        return builder.build();
    }
}
