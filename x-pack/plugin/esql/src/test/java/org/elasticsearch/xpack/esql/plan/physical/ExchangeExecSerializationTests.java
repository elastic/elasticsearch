/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ExchangeExecSerializationTests extends AbstractPhysicalPlanSerializationTests<ExchangeExec> {
    static ExchangeExec randomExchangeExec(int depth) {
        Source source = randomSource();
        List<Attribute> output = randomFieldAttributes(1, 5, false);
        boolean inBetweenAggs = randomBoolean();
        ExchangeExec.Scope scope = randomFrom(ExchangeExec.Scope.values());
        PhysicalPlan child = randomChild(depth);
        return new ExchangeExec(source, output, inBetweenAggs, scope, child);
    }

    @Override
    protected ExchangeExec createTestInstance() {
        return randomExchangeExec(0);
    }

    @Override
    protected ExchangeExec mutateInstance(ExchangeExec instance) throws IOException {
        List<Attribute> output = instance.output();
        boolean inBetweenAggs = instance.inBetweenAggs();
        ExchangeExec.Scope scope = instance.scope();
        PhysicalPlan child = instance.child();
        switch (between(0, 3)) {
            case 0 -> output = randomValueOtherThan(output, () -> randomFieldAttributes(1, 5, false));
            case 1 -> inBetweenAggs = false == inBetweenAggs;
            case 2 -> scope = scope == ExchangeExec.Scope.CLUSTER ? ExchangeExec.Scope.NODE : ExchangeExec.Scope.CLUSTER;
            case 3 -> child = randomValueOtherThan(child, () -> randomChild(0));
        }
        return new ExchangeExec(instance.source(), output, inBetweenAggs, scope, child);
    }

    /** A node that predates the scope reads every exchange as a cluster exchange, which is what it always was. */
    public void testClusterScopeToOldNode() throws IOException {
        TransportVersion old = TransportVersionUtils.getPreviousVersion(DataType.DataTypesTransportVersions.ESQL_FETCH_PHASE_PLAN);
        ExchangeExec exchange = new ExchangeExec(
            Source.EMPTY,
            randomFieldAttributes(1, 5, false, old),
            randomBoolean(),
            ExchangeExec.Scope.CLUSTER,
            new ExchangeSourceExec(Source.EMPTY, randomFieldAttributes(1, 5, false, old), randomBoolean())
        );
        assertThat(copyInstance(exchange, old), equalTo(exchange));
    }

    /** A node scope is never sent to a node that cannot read it. The planner prevents it, the sender fails if it happens. */
    public void testNodeScopeToOldNodeFails() {
        TransportVersion old = TransportVersionUtils.getPreviousVersion(DataType.DataTypesTransportVersions.ESQL_FETCH_PHASE_PLAN);
        ExchangeExec exchange = new ExchangeExec(
            Source.EMPTY,
            randomFieldAttributes(1, 5, false, old),
            randomBoolean(),
            ExchangeExec.Scope.NODE,
            new ExchangeSourceExec(Source.EMPTY, randomFieldAttributes(1, 5, false, old), randomBoolean())
        );
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> copyInstance(exchange, old));
        assertThat(e.getMessage(), containsString("doesn't understand NODE exchanges"));
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
