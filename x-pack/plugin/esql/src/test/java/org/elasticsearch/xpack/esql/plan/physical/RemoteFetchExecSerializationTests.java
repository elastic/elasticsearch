/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;

public class RemoteFetchExecSerializationTests extends AbstractPhysicalPlanSerializationTests<RemoteFetchExec> {
    public static RemoteFetchExec randomRemoteFetchExec(int depth) {
        Source source = randomSource();
        PhysicalPlan child = randomChild(depth);
        Attribute handleAttribute = randomFieldAttributes(1, 1, false).get(0);
        List<Attribute> attributesToFetch = randomFieldAttributes(1, 4, false);
        List<Attribute> fetchedOutputAttributes = randomFieldAttributes(1, 4, false);
        PhysicalPlan pushdownPlan = randomBoolean() ? null : randomChild(depth);
        return new RemoteFetchExec(source, child, handleAttribute, attributesToFetch, fetchedOutputAttributes, pushdownPlan);
    }

    @Override
    protected RemoteFetchExec createTestInstance() {
        return randomRemoteFetchExec(0);
    }

    @Override
    protected RemoteFetchExec mutateInstance(RemoteFetchExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        Attribute handleAttribute = instance.handleAttribute();
        List<Attribute> attributesToFetch = instance.attributesToFetch();
        List<Attribute> fetchedOutputAttributes = instance.fetchedOutputAttributes();
        PhysicalPlan pushdownPlan = instance.pushdownPlan();
        switch (between(0, 4)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> handleAttribute = randomValueOtherThan(handleAttribute, () -> randomFieldAttributes(1, 1, false).get(0));
            case 2 -> attributesToFetch = randomValueOtherThan(attributesToFetch, () -> randomFieldAttributes(1, 4, false));
            case 3 -> fetchedOutputAttributes = randomValueOtherThan(fetchedOutputAttributes, () -> randomFieldAttributes(1, 4, false));
            case 4 -> pushdownPlan = randomValueOtherThan(pushdownPlan, () -> randomBoolean() ? null : randomChild(0));
            default -> throw new AssertionError("unexpected mutation branch");
        }
        return new RemoteFetchExec(instance.source(), child, handleAttribute, attributesToFetch, fetchedOutputAttributes, pushdownPlan);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
