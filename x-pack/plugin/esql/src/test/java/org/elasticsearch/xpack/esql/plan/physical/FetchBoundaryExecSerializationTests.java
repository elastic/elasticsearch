/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Verifies the serialized coordinator-to-data-node fetch handoff because it also controls schema and retained-context requirements.
 */
public class FetchBoundaryExecSerializationTests extends AbstractPhysicalPlanSerializationTests<FetchBoundaryExec> {

    @Override
    protected FetchBoundaryExec createTestInstance() {
        PhysicalPlan child = randomChild(0);
        Attribute handle = randomFieldAttributes(1, 1, false).getFirst();
        List<Attribute> output = new ArrayList<>(randomFieldAttributes(0, 4, false));
        output.add(0, handle);
        return new FetchBoundaryExec(randomSource(), child, handle, output);
    }

    public void testDefinesFetchHandoffContract() {
        FetchBoundaryExec boundary = createTestInstance();

        assertThat(boundary.output(), equalTo(boundary.handoffOutput()));
        assertTrue(boundary.output().contains(boundary.handleAttribute()));
        assertTrue(boundary.requiresRetainedSearchContexts());
        assertThat(boundary.minimumTransportVersion(), equalTo(FetchBoundaryExec.ESQL_FETCH_BOUNDARY));

        String plan = boundary.toString();
        assertThat(plan, containsString("FetchBoundaryExec"));
        assertThat(plan, containsString("handle="));
        assertThat(plan, containsString("handoffOutput="));
        assertThat(plan, containsString("requiresRetainedSearchContexts=true"));
    }

    public void testRejectsHandoffWithoutHandle() {
        PhysicalPlan child = randomChild(0);
        Attribute handle = randomFieldAttributes(1, 1, false).getFirst();
        List<Attribute> output = randomFieldAttributes(1, 4, false);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new FetchBoundaryExec(randomSource(), child, handle, output)
        );
        assertThat(e.getMessage(), containsString("fetch handoff output must contain handle attribute"));
    }

    public void testContractIsReducerAgnostic() {
        Attribute handle = randomFieldAttributes(1, 1, false).getFirst();
        FetchBoundaryExec boundary = new FetchBoundaryExec(
            randomSource(),
            new ExchangeSourceExec(randomSource(), List.of(), false),
            handle,
            List.of(handle)
        );

        assertThat(boundary.toString(), not(containsString("TopN")));
        assertThat(boundary.handoffOutput(), equalTo(List.of(handle)));
    }

    @Override
    protected FetchBoundaryExec mutateInstance(FetchBoundaryExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        Attribute handle = instance.handleAttribute();
        List<Attribute> output = instance.handoffOutput();
        switch (between(0, 2)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> {
                Attribute previousHandle = handle;
                Attribute newHandle = randomValueOtherThan(handle, () -> randomFieldAttributes(1, 1, false).getFirst());
                output = output.stream().map(attribute -> attribute.equals(previousHandle) ? newHandle : attribute).toList();
                handle = newHandle;
            }
            case 2 -> {
                output = new ArrayList<>(randomFieldAttributes(1, 4, false));
                output.add(0, handle);
            }
            default -> throw new AssertionError("unexpected mutation branch");
        }
        return new FetchBoundaryExec(instance.source(), child, handle, output);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
