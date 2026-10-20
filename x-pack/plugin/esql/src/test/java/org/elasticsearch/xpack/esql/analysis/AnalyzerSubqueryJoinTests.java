/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.VersionMode;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn.ExecuteLocation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.join.SubqueryJoin;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.indexWithDateDateNanosUnionType;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class AnalyzerSubqueryJoinTests extends AnalyzerTestCase {

    public AnalyzerSubqueryJoinTests(VersionMode versionMode) {
        super(versionMode);
    }

    public void testResolvesKeysAndPinsUnprojectedSubqueryOutput() {
        assumeTrue("JOIN on a subquery required", EsqlCapabilities.Cap.JOIN_ON_SUBQUERY.isEnabled());

        LogicalPlan plan = analyzer().addEmployees().query("""
            ROW emp_no = 10001
            | LEFT OUTER JOIN (FROM employees | WHERE emp_no == 10001) ON emp_no
            | LIMIT 10
            """);
        SubqueryJoin join = findSubqueryJoin(plan);

        assertTrue(join.config().leftFields().getFirst().resolved());
        assertTrue(join.config().rightFields().getFirst().resolved());
        assertThat(join.right(), instanceOf(Project.class));
        Attribute projectedKey = join.right()
            .output()
            .stream()
            .filter(attribute -> attribute.name().equals("emp_no"))
            .findFirst()
            .orElseThrow();
        assertThat(join.config().rightFields().getFirst().id(), equalTo(projectedKey.id()));
    }

    public void testNumericJoinKeysRemainCompatible() {
        assumeTrue("JOIN on a subquery required", EsqlCapabilities.Cap.JOIN_ON_SUBQUERY.isEnabled());
        LogicalPlan plan = analyzer().query("ROW key = 1 | LEFT OUTER JOIN (ROW key = 1.0, value = 2) ON key | LIMIT 10");
        SubqueryJoin join = findSubqueryJoin(plan);
        assertThat(join.config().leftFields().getFirst().dataType(), equalTo(DataType.INTEGER));
        assertThat(join.config().rightFields().getFirst().dataType(), equalTo(DataType.DOUBLE));
    }

    public void testUnsignedLongAndDoubleJoinKeysAreIncompatible() {
        assumeTrue("JOIN on a subquery required", EsqlCapabilities.Cap.JOIN_ON_SUBQUERY.isEnabled());
        analyzer().error(
            "ROW key = 1::unsigned_long | LEFT OUTER JOIN (ROW key = 1.0, value = 2) ON key | LIMIT 10",
            containsString("is incompatible with right field")
        );
    }

    public void testInnerJoinSubqueryExecutesOnCoordinator() {
        assumeTrue("JOIN on a subquery required", EsqlCapabilities.Cap.JOIN_ON_SUBQUERY.isEnabled());
        LogicalPlan plan = analyzer().query("ROW key = 1 | INNER JOIN (ROW key = 1, value = 2) ON key | LIMIT 10");

        assertThat(findSubqueryJoin(plan).executesOn(), equalTo(ExecuteLocation.COORDINATOR));
    }

    public void testDateAndDateNanosJoinKeysAreIncompatible() {
        assumeTrue("JOIN on a subquery required", EsqlCapabilities.Cap.JOIN_ON_SUBQUERY.isEnabled());
        analyzer().error("""
            ROW key = TO_DATETIME("2023-01-01T00:00:00Z")
            | LEFT OUTER JOIN (ROW key = TO_DATE_NANOS("2023-01-01T00:00:00.123456789Z")) ON key
            | LIMIT 10
            """, containsString("is incompatible with right field"));
    }

    public void testUnionTypeMustBeConvertedInsideSubquery() {
        assumeTrue("JOIN on a subquery required", EsqlCapabilities.Cap.JOIN_ON_SUBQUERY.isEnabled());

        analyzer().addIndex(indexWithDateDateNanosUnionType()).error("""
            FROM index*
            | EVAL join_key = 1
            | LEFT OUTER JOIN (
                FROM index*
                | EVAL join_key = 1
                | KEEP join_key, date_and_date_nanos
              ) ON join_key
            | EVAL converted = date_and_date_nanos::date_nanos
            | LIMIT 10
            """, containsString("must be converted inside the subquery"));

        LogicalPlan plan = analyzer().addIndex(indexWithDateDateNanosUnionType()).query("""
            FROM index*
            | EVAL join_key = 1
            | LEFT OUTER JOIN (
                FROM index*
                | EVAL join_key = 1, converted = date_and_date_nanos::date_nanos
                | KEEP join_key, converted
              ) ON join_key
            | KEEP converted
            | LIMIT 10
            """);
        Attribute converted = plan.output().stream().filter(attribute -> attribute.name().equals("converted")).findFirst().orElseThrow();
        assertThat(converted.dataType(), equalTo(DataType.DATE_NANOS));
    }

    public void testExecutionModeUsesRemoteScopeAndMinimumTransportVersion() {
        TransportVersion subqueryVersion = Analyzer.ESQL_JOIN_SUBQUERY;
        TransportVersion olderVersion = TransportVersionUtils.getPreviousVersion(subqueryVersion);
        EsRelation local = relation(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
        EsRelation remote = relation("remote");

        assertThat(Analyzer.ResolveRefs.newSubqueryJoinMode(subqueryVersion, local), equalTo(ExecuteLocation.ANY));
        assertThat(Analyzer.ResolveRefs.newSubqueryJoinMode(olderVersion, local), equalTo(ExecuteLocation.COORDINATOR));
        assertThat(Analyzer.ResolveRefs.newSubqueryJoinMode(subqueryVersion, remote), equalTo(ExecuteLocation.COORDINATOR));

        SubqueryJoin nested = new SubqueryJoin(Source.EMPTY, local, remote, List.of());
        assertThat(Analyzer.ResolveRefs.newSubqueryJoinMode(subqueryVersion, nested), equalTo(ExecuteLocation.ANY));
    }

    private static SubqueryJoin findSubqueryJoin(LogicalPlan plan) {
        return (SubqueryJoin) plan.collect(SubqueryJoin.class::isInstance).getFirst();
    }

    private static EsRelation relation(String clusterAlias) {
        return new EsRelation(Source.EMPTY, "idx", IndexMode.STANDARD, Map.of(), Map.of(clusterAlias, List.of("idx")), Map.of(), List.of());
    }
}
