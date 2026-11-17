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
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.expression.function.ReferenceAttributeTestUtils.randomReferenceAttribute;

public class NestedExpandExecSerializationTests extends AbstractPhysicalPlanSerializationTests<NestedExpandExec> {
    @Override
    protected NestedExpandExec createTestInstance() {
        Source source = randomSource();
        PhysicalPlan child = randomChild(0);
        int size = between(0, 3);
        return new NestedExpandExec(source, child, randomAlphaOfLength(5), randomExpanded(size), randomPaths(size));
    }

    @Override
    protected NestedExpandExec mutateInstance(NestedExpandExec instance) throws IOException {
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
        return new NestedExpandExec(instance.source(), child, path, expanded, paths);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
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
