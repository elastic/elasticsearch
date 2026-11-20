/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.UnionTypeEsField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.analysis.AnalyzerNestedExpandTests.nestedAnalyzer;
import static org.elasticsearch.xpack.esql.expression.function.ReferenceAttributeTestUtils.randomReferenceAttribute;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

/** Only a bound {@link NestedFlatten} is serialized, and it is read back without the name the user wrote. */
public class NestedFlattenSerializationTests extends AbstractLogicalPlanSerializationTests<NestedFlatten> {
    @Override
    protected NestedFlatten createTestInstance() {
        Source source = randomSource();
        LogicalPlan child = randomChild(0);
        int size = between(0, 3);
        return new NestedFlatten(source, child, null, randomAlphaOfLength(5), randomExpanded(size), randomPaths(size));
    }

    @Override
    protected NestedFlatten mutateInstance(NestedFlatten instance) throws IOException {
        LogicalPlan child = instance.child();
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
        return new NestedFlatten(instance.source(), child, null, path, expanded, paths);
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

    public void testRoundTripsChildrenTheTypesOfTwoIndicesMerged() throws IOException {
        SerializationTestUtils.assertSerialization(analyzedFlatten("FROM ne_types | NESTED_FLATTEN items | KEEP items.small | LIMIT 10"));
    }

    public void testRoundTripsAChildTheFlatteningConverts() throws IOException {
        NestedFlatten flatten = analyzedFlatten("FROM ne_types | NESTED_FLATTEN items | EVAL x = TO_LONG(items.v) | KEEP x | LIMIT 10");
        Attribute converted = flatten.expanded().stream().filter(a -> a.name().contains("converted_to")).findFirst().orElseThrow();
        assertThat(((FieldAttribute) converted).field(), instanceOf(UnionTypeEsField.class));
        SerializationTestUtils.assertSerialization(flatten);
    }

    public static NestedFlatten analyzedFlatten(String query) {
        assumeTrue("NESTED_FLATTEN is snapshot-only", EsqlCapabilities.Cap.NESTED_FLATTEN.isEnabled());
        List<NestedFlatten> found = new ArrayList<>();
        nestedAnalyzer(NestedFlatten.VERSION).query(query).forEachDown(NestedFlatten.class, found::add);
        return found.getFirst();
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
