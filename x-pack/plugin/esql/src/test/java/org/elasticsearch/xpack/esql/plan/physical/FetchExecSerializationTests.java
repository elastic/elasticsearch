/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.FetchSource;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.planner.FieldExtractionSpec;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

/**
 * Verifies the serialized coordinator-side fetch operation and the constrained data-node plan it carries for deferred field loading.
 */
public class FetchExecSerializationTests extends AbstractPhysicalPlanSerializationTests<FetchExec> {
    private static final DataType[] FETCH_TYPES = Arrays.stream(DataType.values())
        .filter(FieldExtractionSpec::supportsDirectDataType)
        .toArray(DataType[]::new);

    private static List<Attribute> randomFetchAttributes() {
        return randomList(1, 4, () -> new ReferenceAttribute(randomSource(), null, randomAlphaOfLength(8), randomFrom(FETCH_TYPES)));
    }

    private static List<FieldExtractionSpec> extractionSpecs(List<Attribute> attributes) {
        return attributes.stream().map(attribute -> FieldExtractionSpec.direct(attribute.name(), attribute.dataType())).toList();
    }

    private static FragmentExec randomFetchPlan() {
        FetchSource fetchSource = new FetchSource(randomSource(), randomFieldAttributes(1, 4, false));
        return new FragmentExec(randomSource(), fetchSource, null, between(0, Integer.MAX_VALUE));
    }

    public static FetchExec randomFetchExec(int depth) {
        Source source = randomSource();
        PhysicalPlan child = randomChild(depth);
        Attribute handleAttribute = randomFieldAttributes(1, 1, false).get(0);
        List<Attribute> attributesToFetch = randomFetchAttributes();
        List<Attribute> fetchedOutputAttributes = randomFieldAttributes(1, 4, false);
        FragmentExec fetchPlan = randomFetchPlan();
        return new FetchExec(
            source,
            child,
            handleAttribute,
            attributesToFetch,
            extractionSpecs(attributesToFetch),
            fetchedOutputAttributes,
            fetchPlan
        );
    }

    @Override
    protected FetchExec createTestInstance() {
        return randomFetchExec(0);
    }

    public void testAllowsPushdownFetchPlan() {
        PhysicalPlan child = randomChild(0);
        Attribute handleAttribute = randomFieldAttributes(1, 1, false).get(0);
        List<Attribute> attributesToFetch = randomFetchAttributes();
        FetchSource fetchSource = new FetchSource(randomSource(), attributesToFetch);
        FragmentExec fetchPlan = new FragmentExec(new Project(randomSource(), fetchSource, attributesToFetch));

        FetchExec exec = new FetchExec(
            Source.EMPTY,
            child,
            handleAttribute,
            attributesToFetch,
            extractionSpecs(attributesToFetch),
            attributesToFetch,
            fetchPlan
        );

        assertSame(fetchPlan, exec.fetchPlan());
        assertSame(fetchPlan, exec.pushdownPlan());
    }

    public void testBareFetchSourceDoesNotRequirePushdown() {
        FetchExec exec = randomFetchExec(0);

        assertNull(exec.pushdownPlan());
    }

    public void testRejectsFetchPlanWithoutFetchSource() {
        PhysicalPlan child = randomChild(0);
        Attribute handleAttribute = randomFieldAttributes(1, 1, false).get(0);
        List<Attribute> attributesToFetch = randomFetchAttributes();
        FragmentExec fetchPlan = new FragmentExec(new LocalRelation(Source.EMPTY, attributesToFetch, null));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new FetchExec(
                Source.EMPTY,
                child,
                handleAttribute,
                attributesToFetch,
                extractionSpecs(attributesToFetch),
                attributesToFetch,
                fetchPlan
            )
        );
        assertThat(e.getMessage(), containsString("fetch plan must contain FetchSource"));
    }

    public void testRejectsUnsupportedPushdownNode() {
        PhysicalPlan child = randomChild(0);
        Attribute handleAttribute = randomFieldAttributes(1, 1, false).get(0);
        List<Attribute> attributesToFetch = randomFetchAttributes();
        FetchSource fetchSource = new FetchSource(randomSource(), attributesToFetch);
        FragmentExec fetchPlan = new FragmentExec(new Limit(randomSource(), EsqlTestUtils.of(10), fetchSource));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new FetchExec(
                Source.EMPTY,
                child,
                handleAttribute,
                attributesToFetch,
                extractionSpecs(attributesToFetch),
                attributesToFetch,
                fetchPlan
            )
        );
        assertThat(e.getMessage(), containsString("unsupported fetch pushdown plan [Limit]"));
    }

    public void testRejectsExtractionSpecificationCountMismatch() {
        PhysicalPlan child = randomChild(0);
        Attribute handleAttribute = randomFieldAttributes(1, 1, false).getFirst();
        List<Attribute> attributesToFetch = randomFetchAttributes();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new FetchExec(Source.EMPTY, child, handleAttribute, attributesToFetch, List.of(), attributesToFetch, randomFetchPlan())
        );

        assertThat(e.getMessage(), containsString("must match extraction specifications"));
    }

    public void testRejectsExtractionSpecificationTypeMismatch() {
        PhysicalPlan child = randomChild(0);
        Attribute handleAttribute = randomFieldAttributes(1, 1, false).getFirst();
        Attribute attribute = new ReferenceAttribute(Source.EMPTY, null, "salary", DataType.INTEGER);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new FetchExec(
                Source.EMPTY,
                child,
                handleAttribute,
                List.of(attribute),
                List.of(FieldExtractionSpec.direct("salary", DataType.LONG)),
                List.of(attribute),
                randomFetchPlan()
            )
        );

        assertThat(e.getMessage(), containsString("has type [integer] but extraction specification has type [long]"));
    }

    @Override
    protected FetchExec mutateInstance(FetchExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        Attribute handleAttribute = instance.handleAttribute();
        List<Attribute> attributesToFetch = instance.attributesToFetch();
        List<FieldExtractionSpec> extractionSpecs = instance.extractionSpecs();
        List<Attribute> fetchedOutputAttributes = instance.fetchedOutputAttributes();
        FragmentExec fetchPlan = instance.fetchPlan();
        switch (between(0, 4)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> handleAttribute = randomValueOtherThan(handleAttribute, () -> randomFieldAttributes(1, 1, false).get(0));
            case 2 -> {
                attributesToFetch = randomValueOtherThan(attributesToFetch, FetchExecSerializationTests::randomFetchAttributes);
                extractionSpecs = extractionSpecs(attributesToFetch);
            }
            case 3 -> fetchedOutputAttributes = randomValueOtherThan(fetchedOutputAttributes, () -> randomFieldAttributes(1, 4, false));
            case 4 -> fetchPlan = randomValueOtherThan(fetchPlan, FetchExecSerializationTests::randomFetchPlan);
            default -> throw new AssertionError("unexpected mutation branch");
        }
        return new FetchExec(
            instance.source(),
            child,
            handleAttribute,
            attributesToFetch,
            extractionSpecs,
            fetchedOutputAttributes,
            fetchPlan
        );
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
