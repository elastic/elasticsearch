/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.util.List;

import static org.hamcrest.Matchers.containsString;

/**
 * Checks the generated data-node fragments, not just whether a translator accepts an old minimum version.
 * Versioned round trips use today's readers; mixed-cluster tests must also exercise actual old node binaries.
 */
public class PromqlTransportCompatibilityTests extends ESTestCase {
    private final TransportVersion version;

    @ParametersFactory(argumentFormatting = "version=%1$s")
    public static List<Object[]> parameters() {
        return List.of(
            new Object[] { TransportVersionUtils.getPreviousVersion(FieldAttribute.ESQL_PROMQL_LABEL_RECORD) },
            new Object[] { TransportVersion.current() }
        );
    }

    public PromqlTransportCompatibilityTests(@Name("version") TransportVersion version) {
        this.version = version;
    }

    public void testSelector() {
        assertDataNodeFragments(version, "network.cost");
    }

    public void testWithout() {
        assertDataNodeFragments(version, "sum without (pod) (network.cost)");
    }

    public void testNamedReadAfterWithout() {
        assertDataNodeFragments(version, "sum by (pod) (sum without (pod) (network.cost))");
    }

    public void testWithoutAfterRanking() {
        assertDataNodeFragments(version, "sum without (pod) (topk(2, network.cost))");
    }

    public void testLabelReplace() {
        assertDataNodeFragments(version, "sum by (dst) (label_replace(network.cost, \"dst\", \"$1\", \"pod\", \"(.+)\"))");
    }

    public void testLabelJoin() {
        assertDataNodeFragments(version, "sum by (dst) (label_join(network.cost, \"dst\", \"-\", \"pod\", \"region\"))");
    }

    public void testNestedWithoutRemainsUnsupported() {
        var error = expectThrows(
            VerificationException.class,
            () -> assertDataNodeFragments(version, "sum without (region) (sum without (pod) (network.cost))")
        );
        assertThat(error.getMessage(), containsString("nested WITHOUT over WITHOUT is not supported"));
    }

    private static void assertDataNodeFragments(TransportVersion version, String expression) {
        String query = "PROMQL index=k8s start=\"2024-05-10T00:00:00Z\" end=\"2024-05-10T01:00:00Z\" step=1m (" + expression + ")";
        var analyzed = EsqlTestUtils.analyzer()
            .addK8s()
            .unmappedResolution(UnmappedResolution.NULLIFY)
            .minimumTransportVersion(version)
            .query(query);
        var optimized = new LogicalPlanOptimizer(new LogicalOptimizerContext(EsqlTestUtils.TEST_CFG, FoldContext.small(), version))
            .optimize(analyzed);
        var physical = new PhysicalPlanOptimizer(new PhysicalOptimizerContext(EsqlTestUtils.TEST_CFG, version)).optimize(
            new Mapper().map(new Versioned<>(optimized, version))
        );
        var fragments = physical.collect(FragmentExec.class);
        assertFalse(query, fragments.isEmpty());
        for (FragmentExec fragment : fragments) {
            fragment.fragment().forEachExpressionDown(Expression.class, value -> {
                if (value instanceof VersionedNamedWriteable writeable) {
                    assertTrue(query + ": " + value, writeable.supportsVersion(version));
                }
            });
            SerializationTestUtils.serializeDeserialize(fragment.fragment(), (out, plan) -> {
                out.setTransportVersion(version);
                out.writeNamedWriteable(plan);
                out.writeInt(42);
            }, in -> {
                in.setTransportVersion(version);
                LogicalPlan restored = in.readNamedWriteable(LogicalPlan.class);
                assertEquals(query, 42, in.readInt());
                assertEquals(query, -1, in.read());
                return restored;
            });
        }
    }
}
