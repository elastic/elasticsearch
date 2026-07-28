/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.AbstractNamedWriteableTestCase;

import java.util.List;

public class UnmappedFieldsPatternTests extends AbstractNamedWriteableTestCase<UnmappedFieldsPattern> {

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(List.of(UnmappedFieldsPattern.ENTRY));
    }

    @Override
    protected Class<UnmappedFieldsPattern> categoryClass() {
        return UnmappedFieldsPattern.class;
    }

    @Override
    protected UnmappedFieldsPattern createTestInstance() {
        return switch (between(0, 3)) {
            case 0 -> UnmappedFieldsPattern.ALL;
            case 1 -> UnmappedFieldsPattern.NONE;
            case 2 -> UnmappedFieldsPattern.includes(List.of("first*", "given*"))
                .intersect(UnmappedFieldsPattern.includes(List.of("last*", "family*")))
                .withAdditionalExcludes(List.of("secret*", "emp_no"));
            case 3 -> UnmappedFieldsPattern.excludes(List.of(randomAlphaOfLength(4) + "*"));
            default -> throw new AssertionError("unreachable");
        };
    }

    @Override
    protected UnmappedFieldsPattern mutateInstance(UnmappedFieldsPattern instance) {
        if (instance.isNone()) {
            return UnmappedFieldsPattern.ALL;
        }
        if (instance.equals(UnmappedFieldsPattern.ALL)) {
            return UnmappedFieldsPattern.NONE;
        }
        return randomBoolean()
            ? instance.intersect(UnmappedFieldsPattern.includes(List.of("mutation_" + randomAlphaOfLength(4) + "*")))
            : instance.withAdditionalExcludes(List.of("mutation_" + randomAlphaOfLength(4)));
    }
}
