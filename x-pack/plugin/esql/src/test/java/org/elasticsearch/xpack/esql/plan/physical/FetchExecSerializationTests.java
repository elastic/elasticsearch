/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.FetchSource;

import java.io.IOException;
import java.util.List;

public class FetchExecSerializationTests extends AbstractPhysicalPlanSerializationTests<FetchExec> {
    private static FragmentExec randomFetchPlan() {
        FetchSource fetchSource = new FetchSource(randomSource(), randomFieldAttributes(1, 4, false));
        return new FragmentExec(randomSource(), fetchSource, null, between(0, Integer.MAX_VALUE));
    }

    public static FetchExec randomFetchExec(int depth) {
        Source source = randomSource();
        PhysicalPlan child = randomChild(depth);
        Attribute handleAttribute = randomFieldAttributes(1, 1, false).get(0);
        List<Attribute> attributesToFetch = randomFieldAttributes(1, 4, false);
        List<Attribute> fetchedOutputAttributes = randomFieldAttributes(1, 4, false);
        FragmentExec fetchPlan = randomFetchPlan();
        return new FetchExec(source, child, handleAttribute, attributesToFetch, fetchedOutputAttributes, fetchPlan);
    }

    @Override
    protected FetchExec createTestInstance() {
        return randomFetchExec(0);
    }

    @Override
    protected FetchExec mutateInstance(FetchExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        Attribute handleAttribute = instance.handleAttribute();
        List<Attribute> attributesToFetch = instance.attributesToFetch();
        List<Attribute> fetchedOutputAttributes = instance.fetchedOutputAttributes();
        FragmentExec fetchPlan = instance.fetchPlan();
        switch (between(0, 4)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> handleAttribute = randomValueOtherThan(handleAttribute, () -> randomFieldAttributes(1, 1, false).get(0));
            case 2 -> attributesToFetch = randomValueOtherThan(attributesToFetch, () -> randomFieldAttributes(1, 4, false));
            case 3 -> fetchedOutputAttributes = randomValueOtherThan(fetchedOutputAttributes, () -> randomFieldAttributes(1, 4, false));
            case 4 -> fetchPlan = randomValueOtherThan(fetchPlan, FetchExecSerializationTests::randomFetchPlan);
            default -> throw new AssertionError("unexpected mutation branch");
        }
        return new FetchExec(instance.source(), child, handleAttribute, attributesToFetch, fetchedOutputAttributes, fetchPlan);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
