/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.NestedContainerField;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class NestedContainerFieldTests extends AbstractEsFieldTypeTests<NestedContainerField> {
    @Override
    protected NestedContainerField createTestInstance() {
        int children = between(1, 4);
        List<String> paths = java.util.stream.IntStream.range(0, children).mapToObj(i -> randomAlphaOfLength(3) + i).toList();
        List<DataType> types = java.util.stream.IntStream.range(0, children)
            .mapToObj(i -> randomFrom(DataType.KEYWORD, DataType.LONG, DataType.DOUBLE, DataType.BOOLEAN, DataType.GEO_POINT))
            .toList();
        return new NestedContainerField(randomAlphaOfLength(5), randomAlphaOfLength(4), paths, types);
    }

    @Override
    protected NestedContainerField mutateInstance(NestedContainerField instance) {
        return switch (between(0, 2)) {
            case 0 -> new NestedContainerField(instance.getName() + "x", instance.path(), instance.paths(), instance.types());
            case 1 -> new NestedContainerField(instance.getName(), instance.path() + "x", instance.paths(), instance.types());
            case 2 -> new NestedContainerField(
                instance.getName(),
                instance.path(),
                java.util.stream.Stream.concat(instance.paths().stream(), java.util.stream.Stream.of("extra")).toList(),
                java.util.stream.Stream.concat(instance.types().stream(), java.util.stream.Stream.of(DataType.LONG)).toList()
            );
            default -> throw new IllegalArgumentException();
        };
    }

    public void testKeepsSourceType() {
        assertThat(createTestInstance().getDataType(), equalTo(DataType.SOURCE));
    }

    public void testRefusesAnOlderWireVersion() {
        NestedContainerField field = createTestInstance();
        expectThrows(IOException.class, () -> copyInstance(field, TransportVersionUtils.getPreviousVersion(NestedContainerField.VERSION)));
        assertThat(field.paths().size(), is(field.types().size()));
    }
}
