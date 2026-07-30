/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RemoteFetchSource;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class RemoteFetchExecSerializationTests extends AbstractPhysicalPlanSerializationTests<RemoteFetchExec> {
    private static FragmentExec randomFetchPlan() {
        RemoteFetchSource fetchSource = new RemoteFetchSource(randomSource(), randomFieldAttributes(1, 4, false));
        return new FragmentExec(randomSource(), fetchSource, null, between(0, Integer.MAX_VALUE));
    }

    public static RemoteFetchExec randomRemoteFetchExec(int depth) {
        Source source = randomSource();
        PhysicalPlan child = randomChild(depth);
        Attribute handleAttribute = randomFieldAttributes(1, 1, false).get(0);
        List<Attribute> attributesToFetch = randomFieldAttributes(1, 4, false);
        List<Attribute> fetchedOutputAttributes = randomFieldAttributes(1, 4, false);
        FragmentExec fetchPlan = randomFetchPlan();
        return new RemoteFetchExec(source, child, handleAttribute, attributesToFetch, fetchedOutputAttributes, fetchPlan);
    }

    @Override
    protected RemoteFetchExec createTestInstance() {
        return randomRemoteFetchExec(0);
    }

    public void testAllowsPushdownFetchPlan() {
        PhysicalPlan child = randomChild(0);
        Attribute handleAttribute = randomFieldAttributes(1, 1, false).get(0);
        List<Attribute> attributesToFetch = randomFieldAttributes(1, 4, false);
        RemoteFetchSource fetchSource = new RemoteFetchSource(randomSource(), attributesToFetch);
        FragmentExec fetchPlan = new FragmentExec(new Project(randomSource(), fetchSource, attributesToFetch));

        RemoteFetchExec exec = new RemoteFetchExec(Source.EMPTY, child, handleAttribute, attributesToFetch, attributesToFetch, fetchPlan);

        assertSame(fetchPlan, exec.fetchPlan());
    }

    public void testRejectsFetchPlanWithoutRemoteFetchSource() {
        PhysicalPlan child = randomChild(0);
        Attribute handleAttribute = randomFieldAttributes(1, 1, false).get(0);
        List<Attribute> attributesToFetch = randomFieldAttributes(1, 4, false);
        FragmentExec fetchPlan = new FragmentExec(new LocalRelation(Source.EMPTY, attributesToFetch, null));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new RemoteFetchExec(Source.EMPTY, child, handleAttribute, attributesToFetch, attributesToFetch, fetchPlan)
        );
        assertThat(e.getMessage(), containsString("remote fetch plan must contain RemoteFetchSource"));
    }

    public void testRejectsUnsupportedPushdownNode() {
        PhysicalPlan child = randomChild(0);
        Attribute handleAttribute = randomFieldAttributes(1, 1, false).get(0);
        List<Attribute> attributesToFetch = randomFieldAttributes(1, 4, false);
        RemoteFetchSource fetchSource = new RemoteFetchSource(randomSource(), attributesToFetch);
        FragmentExec fetchPlan = new FragmentExec(new Limit(randomSource(), EsqlTestUtils.of(10), fetchSource));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new RemoteFetchExec(Source.EMPTY, child, handleAttribute, attributesToFetch, attributesToFetch, fetchPlan)
        );
        assertThat(e.getMessage(), containsString("unsupported remote fetch pushdown plan [Limit]"));
    }

    @Override
    protected RemoteFetchExec mutateInstance(RemoteFetchExec instance) throws IOException {
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
            case 4 -> fetchPlan = randomValueOtherThan(fetchPlan, RemoteFetchExecSerializationTests::randomFetchPlan);
            default -> throw new AssertionError("unexpected mutation branch");
        }
        return new RemoteFetchExec(instance.source(), child, handleAttribute, attributesToFetch, fetchedOutputAttributes, fetchPlan);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
