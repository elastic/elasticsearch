/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.UnionTypeEsField;
import org.elasticsearch.xpack.esql.plan.logical.NestedFlatten;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.expression.function.ReferenceAttributeTestUtils.randomReferenceAttribute;
import static org.elasticsearch.xpack.esql.plan.logical.NestedFlattenSerializationTests.analyzedFlatten;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class NestedFlattenExecSerializationTests extends AbstractPhysicalPlanSerializationTests<NestedFlattenExec> {
    @Override
    protected NestedFlattenExec createTestInstance() {
        Source source = randomSource();
        PhysicalPlan child = randomChild(0);
        int size = between(0, 3);
        return new NestedFlattenExec(source, child, randomAlphaOfLength(5), randomExpanded(size), randomPaths(size));
    }

    @Override
    protected NestedFlattenExec mutateInstance(NestedFlattenExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        String path = instance.path();
        List<Attribute> expanded = instance.expanded();
        List<String> paths = instance.paths();
        int size = paths.size();
        switch (size == 0 ? between(0, 2) : between(0, 4)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> path = randomValueOtherThan(path, () -> randomAlphaOfLength(5));
            case 2 -> {
                expanded = randomExpanded(size + 1);
                paths = randomPaths(size + 1);
            }
            case 3 -> expanded = randomValueOtherThan(expanded, () -> randomExpanded(size));
            case 4 -> paths = randomValueOtherThan(paths, () -> randomPaths(size));
            default -> throw new UnsupportedOperationException();
        }
        return new NestedFlattenExec(instance.source(), child, path, expanded, paths);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }

    public void testWriteToRefusesNodesWithoutNestedFlatten() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersionUtils.randomVersionNotSupporting(NestedFlatten.VERSION));
            IOException e = expectThrows(IOException.class, () -> createTestInstance().writeTo(out));
            assertThat(e.getMessage(), containsString("NESTED_FLATTEN requires nested flattening transport support"));
        }
    }

    public void testRoundTripsAChildTheFlatteningConverts() throws IOException {
        NestedFlatten logical = analyzedFlatten("FROM ne_types | NESTED_FLATTEN items | EVAL x = TO_LONG(items.v) | KEEP x | LIMIT 10");
        NestedFlattenExec exec = new NestedFlattenExec(
            logical.source(),
            randomChild(0),
            logical.path(),
            logical.expanded(),
            logical.paths()
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
