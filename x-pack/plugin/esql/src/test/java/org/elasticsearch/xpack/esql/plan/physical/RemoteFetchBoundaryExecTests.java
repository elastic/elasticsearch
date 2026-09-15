/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plugin.RemoteFetchHandle;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.plan.AbstractNodeSerializationTests.randomFieldAttributes;

public class RemoteFetchBoundaryExecTests extends AbstractNodeTestCase<RemoteFetchBoundaryExec, PhysicalPlan> {

    @Override
    protected RemoteFetchBoundaryExec randomInstance() {
        return randomRemoteFetchBoundaryExec();
    }

    public static RemoteFetchBoundaryExec randomRemoteFetchBoundaryExec() {
        Attribute doc = doc();
        Attribute handle = handle();
        List<Attribute> eager = randomFieldAttributes(1, 4, false);
        return boundary(doc, handle, eager, child(doc, eager));
    }

    @Override
    protected RemoteFetchBoundaryExec mutate(RemoteFetchBoundaryExec instance) {
        Attribute doc = instance.documentAttribute();
        Attribute handle = instance.handleAttribute();
        List<Attribute> eager = instance.eagerAttributes();
        PhysicalPlan child = instance.child();
        switch (between(0, 3)) {
            case 0 -> child = new ProjectExec(Source.EMPTY, child, instance.dataOutput());
            case 1 -> {
                doc = doc();
                child = child(doc, eager);
            }
            case 2 -> handle = handle();
            case 3 -> {
                eager = randomValueOtherThan(eager, () -> randomFieldAttributes(1, 4, false));
                child = child(doc, eager);
            }
            default -> throw new AssertionError("unexpected mutation branch");
        }
        return boundary(doc, handle, eager, child);
    }

    @Override
    protected RemoteFetchBoundaryExec copy(RemoteFetchBoundaryExec instance) {
        return boundary(instance.documentAttribute(), instance.handleAttribute(), instance.eagerAttributes(), instance.child());
    }

    @Override
    public void testTransform() {
        RemoteFetchBoundaryExec instance = randomInstance();
        Attribute newHandle = handle();
        RemoteFetchBoundaryExec transformed = (RemoteFetchBoundaryExec) instance.transformPropertiesOnly(
            Object.class,
            property -> Objects.equals(property, instance.handleAttribute()) ? newHandle : property
        );

        assertEquals(boundary(instance.documentAttribute(), newHandle, instance.eagerAttributes(), instance.child()), transformed);
    }

    @Override
    public void testReplaceChildren() {
        RemoteFetchBoundaryExec instance = randomInstance();
        PhysicalPlan newChild = new ProjectExec(Source.EMPTY, instance.child(), instance.dataOutput());

        assertEquals(
            boundary(instance.documentAttribute(), instance.handleAttribute(), instance.eagerAttributes(), newChild),
            instance.replaceChild(newChild)
        );
    }

    private static RemoteFetchBoundaryExec boundary(Attribute doc, Attribute handle, List<Attribute> eager, PhysicalPlan child) {
        return new RemoteFetchBoundaryExec(Source.EMPTY, child, doc, handle, eager);
    }

    private static PhysicalPlan child(Attribute doc, List<Attribute> eager) {
        List<Attribute> output = new ArrayList<>(eager.size() + 1);
        output.add(doc);
        output.addAll(eager);
        return new ExchangeSourceExec(Source.EMPTY, output, false);
    }

    private static Attribute doc() {
        return new MetadataAttribute(Source.EMPTY, MetadataAttribute.DOC, DataType.DOC_DATA_TYPE, false);
    }

    private static Attribute handle() {
        return new ReferenceAttribute(
            Source.EMPTY,
            null,
            RemoteFetchHandle.ATTRIBUTE_NAME,
            DataType.KEYWORD,
            Nullability.FALSE,
            null,
            true
        );
    }
}
