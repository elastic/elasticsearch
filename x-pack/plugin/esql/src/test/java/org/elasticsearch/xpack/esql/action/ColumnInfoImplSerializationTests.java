/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;
import java.util.Map;

public class ColumnInfoImplSerializationTests extends AbstractWireSerializingTestCase<ColumnInfoImpl> {

    @Override
    protected Writeable.Reader<ColumnInfoImpl> instanceReader() {
        return ColumnInfoImpl::new;
    }

    @Override
    protected ColumnInfoImpl createTestInstance() {
        return new ColumnInfoImpl(randomAlphaOfLength(10), randomDataType(), randomOriginalTypes(), randomMeta());
    }

    @Override
    protected ColumnInfoImpl mutateInstance(ColumnInfoImpl in) {
        return switch (between(0, 3)) {
            case 0 -> new ColumnInfoImpl(
                randomValueOtherThan(in.name(), () -> randomAlphaOfLength(10)),
                in.type(),
                in.originalTypes(),
                in.meta()
            );
            case 1 -> new ColumnInfoImpl(in.name(), randomValueOtherThan(in.type(), this::randomDataType), in.originalTypes(), in.meta());
            case 2 -> new ColumnInfoImpl(
                in.name(),
                in.type(),
                randomValueOtherThan(in.originalTypes(), this::randomOriginalTypes),
                in.meta()
            );
            case 3 -> new ColumnInfoImpl(in.name(), in.type(), in.originalTypes(), randomValueOtherThan(in.meta(), this::randomMeta));
            default -> throw new AssertionError("unreachable");
        };
    }

    private DataType randomDataType() {
        return randomFrom(DataType.KEYWORD, DataType.LONG, DataType.DOUBLE, DataType.DATETIME, DataType.DATE_NANOS);
    }

    private List<String> randomOriginalTypes() {
        return randomBoolean() ? null : randomList(1, 4, () -> randomFrom("keyword", "long", "double", "date"));
    }

    private Map<String, Object> randomMeta() {
        if (randomBoolean()) {
            return null;
        }
        return Map.of(
            "bucket",
            Map.of("interval", randomLongBetween(1, 60), "unit", randomFrom("second", "minute", "hour", "day", "month"))
        );
    }
}
