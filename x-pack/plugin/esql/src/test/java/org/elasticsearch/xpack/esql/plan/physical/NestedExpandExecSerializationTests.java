/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.UnionTypeEsField;
import org.elasticsearch.xpack.esql.plan.logical.NestedExpand;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.expression.function.ReferenceAttributeTestUtils.randomReferenceAttribute;
import static org.elasticsearch.xpack.esql.plan.logical.NestedExpandSerializationTests.analyzedExpand;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class NestedExpandExecSerializationTests extends AbstractPhysicalPlanSerializationTests<NestedExpandExec> {
    @Override
    protected NestedExpandExec createTestInstance() {
        Source source = randomSource();
        PhysicalPlan child = randomChild(0);
        int size = between(0, 3);
        return new NestedExpandExec(
            source,
            child,
            randomAlphaOfLength(5),
            randomExpanded(size),
            randomPaths(size),
            randomEstimatedRowSize()
        );
    }

    @Override
    protected NestedExpandExec mutateInstance(NestedExpandExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        String path = instance.path();
        List<Attribute> expanded = instance.expanded();
        List<String> paths = instance.paths();
        Integer estimatedRowSize = instance.estimatedRowSize();
        int size = paths.size();
        switch (size == 0 ? between(0, 3) : between(0, 5)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> path = randomValueOtherThan(path, () -> randomAlphaOfLength(5));
            case 2 -> {
                expanded = randomExpanded(size + 1);
                paths = randomPaths(size + 1);
            }
            case 3 -> estimatedRowSize = randomValueOtherThan(
                estimatedRowSize,
                AbstractPhysicalPlanSerializationTests::randomEstimatedRowSize
            );
            case 4 -> expanded = randomValueOtherThan(expanded, () -> randomExpanded(size));
            case 5 -> paths = randomValueOtherThan(paths, () -> randomPaths(size));
            default -> throw new UnsupportedOperationException();
        }
        return new NestedExpandExec(instance.source(), child, path, expanded, paths, estimatedRowSize);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }

    public void testWriteToRefusesNodesWithoutNestedExpand() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersionUtils.randomVersionNotSupporting(NestedExpand.VERSION));
            IOException e = expectThrows(IOException.class, () -> createTestInstance().writeTo(out));
            assertThat(e.getMessage(), containsString("NESTED_EXPAND requires nested expansion transport support"));
        }
    }

    public void testPeerWithoutRowSizeSupportGetsNoRowSize() throws IOException {
        PhysicalPlan child = randomChild(0);
        NestedExpandExec instance = new NestedExpandExec(Source.EMPTY, child, "items", List.of(), List.of(), between(1, 1000));
        TransportVersion beforeRowSize = TransportVersionUtils.getPreviousVersion(NestedExpandExec.ESTIMATED_ROW_SIZE);
        assertThat(beforeRowSize.supports(NestedExpand.VERSION), is(true));

        NestedExpandExec copy = copyInstance(instance, beforeRowSize);

        assertThat(copy, equalTo(new NestedExpandExec(Source.EMPTY, child, "items", List.of(), List.of(), null)));
    }

    public void testRowSizeRoundTripsWithRowSizeSupport() throws IOException {
        PhysicalPlan child = randomChild(0);
        NestedExpandExec instance = new NestedExpandExec(Source.EMPTY, child, "items", List.of(), List.of(), between(1, 1000));

        NestedExpandExec copy = copyInstance(instance, NestedExpandExec.ESTIMATED_ROW_SIZE);

        assertThat(copy, equalTo(instance));
    }

    public void testRoundTripsAChildTheExpansionConverts() throws IOException {
        NestedExpand logical = analyzedExpand("FROM ne_types | NESTED_EXPAND items | EVAL x = TO_LONG(items.v) | KEEP x | LIMIT 10");
        NestedExpandExec exec = new NestedExpandExec(
            logical.source(),
            randomChild(0),
            logical.path(),
            logical.expanded(),
            logical.paths(),
            randomEstimatedRowSize()
        );
        Attribute converted = exec.expanded().stream().filter(a -> a.name().contains("converted_to")).findFirst().orElseThrow();
        assertThat(((FieldAttribute) converted).field(), instanceOf(UnionTypeEsField.class));
        SerializationTestUtils.assertSerialization(exec);
    }

    private static List<Attribute> randomExpanded(int size) {
        List<Attribute> expanded = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            expanded.add(randomReferenceAttribute(false));
        }
        return expanded;
    }

    private static List<String> randomPaths(int size) {
        List<String> paths = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            paths.add(randomAlphaOfLength(4));
        }
        return paths;
    }
}
