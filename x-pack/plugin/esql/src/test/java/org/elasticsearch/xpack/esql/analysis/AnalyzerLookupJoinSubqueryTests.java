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
import org.elasticsearch.xpack.esql.plan.logical.join.SubqueryLookupJoin;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.indexWithDateDateNanosUnionType;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class AnalyzerLookupJoinSubqueryTests extends AnalyzerTestCase {

    public AnalyzerLookupJoinSubqueryTests(VersionMode versionMode) {
        super(versionMode);
    }

    public void testResolvesKeysAndPinsUnprojectedSubqueryOutput() {
        assumeTrue("JOIN on a subquery required", EsqlCapabilities.Cap.JOIN_ON_SUBQUERY.isEnabled());

        LogicalPlan plan = analyzer().addEmployees().query("""
            ROW emp_no = 10001
            | LEFT OUTER JOIN (FROM employees | WHERE emp_no == 10001) ON emp_no
            | LIMIT 10
            """);
        SubqueryLookupJoin join = findSubqueryLookupJoin(plan);

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
        SubqueryLookupJoin join = findSubqueryLookupJoin(plan);
        assertThat(join.config().leftFields().getFirst().dataType(), equalTo(DataType.INTEGER));
        assertThat(join.config().rightFields().getFirst().dataType(), equalTo(DataType.DOUBLE));
    }

    public void testInnerJoinSubqueryExecutesOnCoordinator() {
        assumeTrue("JOIN on a subquery required", EsqlCapabilities.Cap.JOIN_ON_SUBQUERY.isEnabled());
        LogicalPlan plan = analyzer().query("ROW key = 1 | INNER JOIN (ROW key = 1, value = 2) ON key | LIMIT 10");

        assertThat(findSubqueryLookupJoin(plan).executesOn(), equalTo(ExecuteLocation.COORDINATOR));
    }

    public void testDateAndDateNanosJoinKeysAreIncompatible() {
        assumeTrue("JOIN on a subquery required", EsqlCapabilities.Cap.JOIN_ON_SUBQUERY.isEnabled());
        analyzer().error("""
            ROW key = TO_DATETIME("2023-01-01T00:00:00Z")
            | LEFT OUTER JOIN (ROW key = TO_DATE_NANOS("2023-01-01T00:00:00.123456789Z")) ON key
            | LIMIT 10
            """, containsString("is incompatible with right field"));
    }

    public void testUnionTypeMustBeConvertedInsideLookupSubquery() {
        assumeTrue("JOIN on a subquery required", EsqlCapabilities.Cap.JOIN_ON_SUBQUERY.isEnabled());

        analyzer().addIndex(indexWithDateDateNanosUnionType()).error("""
            FROM index*
            | EVAL lookup_key = 1
            | LEFT OUTER JOIN (
                FROM index*
                | EVAL lookup_key = 1
                | KEEP lookup_key, date_and_date_nanos
              ) ON lookup_key
            | EVAL converted = date_and_date_nanos::date_nanos
            | LIMIT 10
            """, containsString("must be converted inside the subquery"));

        LogicalPlan plan = analyzer().addIndex(indexWithDateDateNanosUnionType()).query("""
            FROM index*
            | EVAL lookup_key = 1
            | LEFT OUTER JOIN (
                FROM index*
                | EVAL lookup_key = 1, converted = date_and_date_nanos::date_nanos
                | KEEP lookup_key, converted
              ) ON lookup_key
            | KEEP converted
            | LIMIT 10
            """);
        Attribute converted = plan.output().stream().filter(attribute -> attribute.name().equals("converted")).findFirst().orElseThrow();
        assertThat(converted.dataType(), equalTo(DataType.DATE_NANOS));
    }

    public void testExecutionModeUsesRemoteScopeAndMinimumTransportVersion() {
        TransportVersion subqueryVersion = Analyzer.ESQL_LOOKUP_JOIN_SUBQUERY;
        TransportVersion olderVersion = TransportVersionUtils.getPreviousVersion(subqueryVersion);
        EsRelation local = relation(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
        EsRelation remote = relation("remote");

        assertThat(Analyzer.ResolveRefs.newSubqueryLookupJoinMode(subqueryVersion, local), equalTo(ExecuteLocation.ANY));
        assertThat(Analyzer.ResolveRefs.newSubqueryLookupJoinMode(olderVersion, local), equalTo(ExecuteLocation.COORDINATOR));
        assertThat(Analyzer.ResolveRefs.newSubqueryLookupJoinMode(subqueryVersion, remote), equalTo(ExecuteLocation.COORDINATOR));

        SubqueryLookupJoin nested = new SubqueryLookupJoin(Source.EMPTY, local, remote, List.of());
        assertThat(Analyzer.ResolveRefs.newSubqueryLookupJoinMode(subqueryVersion, nested), equalTo(ExecuteLocation.ANY));
    }

    private static SubqueryLookupJoin findSubqueryLookupJoin(LogicalPlan plan) {
        return (SubqueryLookupJoin) plan.collect(SubqueryLookupJoin.class::isInstance).getFirst();
    }

    private static EsRelation relation(String clusterAlias) {
        return new EsRelation(Source.EMPTY, "idx", IndexMode.STANDARD, Map.of(), Map.of(clusterAlias, List.of("idx")), Map.of(), List.of());
    }
}
