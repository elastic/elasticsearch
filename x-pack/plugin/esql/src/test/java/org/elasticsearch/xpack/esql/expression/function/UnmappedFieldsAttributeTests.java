/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.AbstractNamedExpressionSerializationTests;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsAttribute;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsPattern;

import java.util.List;
import java.util.Map;

public class UnmappedFieldsAttributeTests extends AbstractNamedExpressionSerializationTests<UnmappedFieldsAttribute> {

    private static UnmappedFieldsPattern patternWithIntersectedOrGroupsAndExcludes() {
        return UnmappedFieldsPattern.includes(List.of("first*", "given*"))
            .intersect(UnmappedFieldsPattern.includes(List.of("last*", "family*")))
            .withAdditionalExcludes(List.of("secret*", "emp_no"));
    }

    private static List<UnmappedFieldsPattern.KeepTerm> randomKeepOrder() {
        return randomBoolean()
            ? List.of()
            : List.of(
                new UnmappedFieldsPattern.KeepTerm("*", true),
                new UnmappedFieldsPattern.KeepTerm(randomAlphaOfLength(4) + "*", true),
                new UnmappedFieldsPattern.KeepTerm(randomAlphaOfLength(4), false)
            );
    }

    private static Map<String, String> randomRenames() {
        return randomBoolean() ? Map.of() : randomMap(1, 3, () -> Tuple.tuple(randomAlphaOfLength(4), randomAlphaOfLength(5)));
    }

    @Override
    protected UnmappedFieldsAttribute createTestInstance() {
        return new UnmappedFieldsAttribute(
            Source.EMPTY,
            DataType.KEYWORD,
            randomFrom(Nullability.values()),
            new NameId(),
            randomBoolean(),
            patternWithIntersectedOrGroupsAndExcludes(),
            randomKeepOrder(),
            randomRenames()
        );
    }

    @Override
    protected UnmappedFieldsAttribute mutateInstance(UnmappedFieldsAttribute instance) {
        Source source = instance.source();
        DataType type = instance.dataType();
        Nullability nullability = instance.nullable();
        NameId id = instance.id();
        boolean synthetic = instance.synthetic();
        UnmappedFieldsPattern pattern = instance.pattern();
        List<UnmappedFieldsPattern.KeepTerm> keepOrder = instance.keepOrder();
        Map<String, String> renames = instance.renames();
        switch (between(0, 6)) {
            case 0 -> type = randomValueOtherThan(type, () -> randomFrom(DataType.types()));
            case 1 -> nullability = randomValueOtherThan(nullability, () -> randomFrom(Nullability.values()));
            case 2 -> id = new NameId();
            case 3 -> synthetic = false == synthetic;
            case 4 -> pattern = randomValueOtherThan(pattern, () -> UnmappedFieldsPattern.excludes(List.of(randomAlphaOfLength(4) + "*")));
            case 5 -> keepOrder = randomValueOtherThan(keepOrder, UnmappedFieldsAttributeTests::randomKeepOrder);
            case 6 -> renames = randomValueOtherThan(renames, UnmappedFieldsAttributeTests::randomRenames);
        }
        return new UnmappedFieldsAttribute(source, type, nullability, id, synthetic, pattern, keepOrder, renames);
    }

    @Override
    protected UnmappedFieldsAttribute mutateNameId(UnmappedFieldsAttribute instance) {
        return new UnmappedFieldsAttribute(
            instance.source(),
            instance.dataType(),
            instance.nullable(),
            new NameId(),
            instance.synthetic(),
            instance.pattern(),
            instance.keepOrder(),
            instance.renames()
        );
    }

    @Override
    protected boolean equalityIgnoresId() {
        return false;
    }
}
