/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.tree.SourceTests;
import org.elasticsearch.xpack.esql.plan.logical.FetchSource;
import org.elasticsearch.xpack.esql.planner.FieldExtractionSpec;
import org.elasticsearch.xpack.esql.tree.EsqlNodeSubclassTests;

import java.util.List;

import static org.elasticsearch.xpack.esql.plan.AbstractNodeSerializationTests.randomFieldAttributes;
import static org.elasticsearch.xpack.esql.plan.physical.AbstractPhysicalPlanSerializationTests.randomChild;

/**
 * Supplies contract-aware construction and mutation for {@link EsqlNodeSubclassTests}.
 */
public class FetchExecTests extends AbstractNodeTestCase<FetchExec, PhysicalPlan> {

    /** Builds a valid fetch node for the generic node-subclass tests. */
    public static FetchExec randomFetchExec() {
        return FetchExecSerializationTests.randomFetchExec(0);
    }

    @Override
    protected FetchExec randomInstance() {
        return randomFetchExec();
    }

    @Override
    protected FetchExec mutate(FetchExec instance) {
        List<FieldExtractionSpec> extractionSpecs = instance.extractionSpecs()
            .stream()
            .map(spec -> FieldExtractionSpec.direct(spec.fieldName() + "_new", spec.dataType(), spec.fieldExtractPreference()))
            .toList();
        return new FetchExec(
            instance.source(),
            instance.child(),
            instance.handleAttribute(),
            instance.attributesToFetch(),
            extractionSpecs,
            instance.fetchedOutputAttributes(),
            instance.fetchPlan()
        );
    }

    @Override
    protected FetchExec copy(FetchExec instance) {
        return copyWith(instance, instance.child(), instance.fetchPlan());
    }

    @Override
    public void testTransform() {
        FetchExec instance = randomInstance();
        Attribute handle = randomValueOtherThan(instance.handleAttribute(), () -> randomFieldAttributes(1, 1, false).getFirst());
        List<Attribute> attributes = renameAttributes(instance.attributesToFetch());
        List<FieldExtractionSpec> extractionSpecs = extractionSpecs(attributes);
        List<Attribute> fetchedOutput = randomValueOtherThan(instance.fetchedOutputAttributes(), () -> randomFieldAttributes(1, 4, false));

        FetchExec transformed = (FetchExec) instance.transformPropertiesOnly(Object.class, property -> {
            if (property == instance.handleAttribute()) {
                return handle;
            }
            if (property == instance.attributesToFetch()) {
                return attributes;
            }
            if (property == instance.extractionSpecs()) {
                return extractionSpecs;
            }
            if (property == instance.fetchedOutputAttributes()) {
                return fetchedOutput;
            }
            return property;
        });

        assertEquals(
            new FetchExec(instance.source(), instance.child(), handle, attributes, extractionSpecs, fetchedOutput, instance.fetchPlan()),
            transformed
        );
    }

    @Override
    public void testReplaceChildren() {
        FetchExec instance = randomInstance();
        PhysicalPlan child = randomValueOtherThan(instance.child(), () -> randomChild(0));
        FragmentExec fetchPlan = randomValueOtherThan(instance.fetchPlan(), () -> randomFetchPlan(instance.attributesToFetch()));

        assertEquals(copyWith(instance, child, fetchPlan), instance.replaceChildren(child, fetchPlan));
    }

    private static FetchExec copyWith(FetchExec instance, PhysicalPlan child, PhysicalPlan fetchPlan) {
        return new FetchExec(
            instance.source(),
            child,
            instance.handleAttribute(),
            instance.attributesToFetch(),
            instance.extractionSpecs(),
            instance.fetchedOutputAttributes(),
            fetchPlan
        );
    }

    private static List<Attribute> renameAttributes(List<Attribute> attributes) {
        return attributes.stream()
            .map(attribute -> new ReferenceAttribute(Source.EMPTY, null, attribute.name() + "_new", attribute.dataType()))
            .map(Attribute.class::cast)
            .toList();
    }

    private static List<FieldExtractionSpec> extractionSpecs(List<Attribute> attributes) {
        return attributes.stream().map(attribute -> FieldExtractionSpec.direct(attribute.name(), attribute.dataType())).toList();
    }

    private static FragmentExec randomFetchPlan(List<Attribute> attributes) {
        return new FragmentExec(new FetchSource(SourceTests.randomSource(), attributes));
    }
}
