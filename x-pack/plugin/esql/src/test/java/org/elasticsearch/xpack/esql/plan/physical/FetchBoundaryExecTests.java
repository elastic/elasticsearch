/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.tree.EsqlNodeSubclassTests;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.plan.AbstractNodeSerializationTests.randomFieldAttributes;
import static org.elasticsearch.xpack.esql.plan.physical.AbstractPhysicalPlanSerializationTests.randomChild;

/**
 * Supplies contract-aware construction and mutation for {@link EsqlNodeSubclassTests}.
 */
public class FetchBoundaryExecTests extends AbstractNodeTestCase<FetchBoundaryExec, PhysicalPlan> {

    public static FetchBoundaryExec randomFetchBoundaryExec() {
        Attribute handle = randomFieldAttributes(1, 1, false).getFirst();
        List<Attribute> handoffOutput = new ArrayList<>(randomFieldAttributes(0, 4, false));
        handoffOutput.add(0, handle);
        return new FetchBoundaryExec(Source.EMPTY, randomChild(0), handle, handoffOutput);
    }

    @Override
    protected FetchBoundaryExec randomInstance() {
        return randomFetchBoundaryExec();
    }

    @Override
    protected FetchBoundaryExec mutate(FetchBoundaryExec instance) {
        return switch (between(0, 2)) {
            case 0 -> new FetchBoundaryExec(
                instance.source(),
                randomValueOtherThan(instance.child(), () -> randomChild(0)),
                instance.handleAttribute(),
                instance.handoffOutput()
            );
            case 1 -> {
                Attribute newHandle = randomValueOtherThan(instance.handleAttribute(), () -> randomFieldAttributes(1, 1, false).getFirst());
                yield new FetchBoundaryExec(instance.source(), instance.child(), newHandle, replaceHandle(instance, newHandle));
            }
            case 2 -> {
                List<Attribute> handoffOutput = new ArrayList<>(instance.handoffOutput());
                handoffOutput.addAll(randomFieldAttributes(1, 4, false));
                yield new FetchBoundaryExec(instance.source(), instance.child(), instance.handleAttribute(), handoffOutput);
            }
            default -> throw new AssertionError("unexpected mutation branch");
        };
    }

    @Override
    protected FetchBoundaryExec copy(FetchBoundaryExec instance) {
        return new FetchBoundaryExec(instance.source(), instance.child(), instance.handleAttribute(), instance.handoffOutput());
    }

    @Override
    public void testTransform() {
        FetchBoundaryExec instance = randomInstance();
        Attribute newHandle = randomValueOtherThan(instance.handleAttribute(), () -> randomFieldAttributes(1, 1, false).getFirst());
        List<Attribute> newHandoffOutput = replaceHandle(instance, newHandle);

        FetchBoundaryExec transformed = (FetchBoundaryExec) instance.transformPropertiesOnly(Object.class, property -> {
            if (Objects.equals(property, instance.handleAttribute())) {
                return newHandle;
            }
            if (Objects.equals(property, instance.handoffOutput())) {
                return newHandoffOutput;
            }
            return property;
        });

        assertEquals(new FetchBoundaryExec(instance.source(), instance.child(), newHandle, newHandoffOutput), transformed);
    }

    @Override
    public void testReplaceChildren() {
        FetchBoundaryExec instance = randomInstance();
        PhysicalPlan newChild = randomValueOtherThan(instance.child(), () -> randomChild(0));

        assertEquals(
            new FetchBoundaryExec(instance.source(), newChild, instance.handleAttribute(), instance.handoffOutput()),
            instance.replaceChild(newChild)
        );
    }

    private static List<Attribute> replaceHandle(FetchBoundaryExec instance, Attribute newHandle) {
        return instance.handoffOutput()
            .stream()
            .map(attribute -> attribute.equals(instance.handleAttribute()) ? newHandle : attribute)
            .toList();
    }
}
