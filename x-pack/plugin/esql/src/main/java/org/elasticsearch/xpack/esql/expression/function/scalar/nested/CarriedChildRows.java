/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.spi.ChildRelationColumns;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.IntPredicate;

/**
 * The children of a dataset's rows, read from the carrier column the scan emitted beside them. The parents' page holds
 * them already, so nothing is read from the source: this only unpacks.
 */
public final class CarriedChildRows implements ChildRows {
    /**
     * @param carrier   evaluates to the carrier column of a parent page
     * @param leafTypes the element type of every leaf the carrier holds, in its order
     * @param wanted    which of those leaves to hand over, in output order
     */
    public record Factory(ExpressionEvaluator.Factory carrier, ElementType[] leafTypes, int[] wanted) implements ChildRows.Factory {
        @Override
        public ChildRows get(DriverContext context) {
            return new CarriedChildRows(context, carrier.get(context), leafTypes, wanted);
        }
    }

    /**
     * The reader of {@code relativePaths} from {@code carrier}, a dataset collection's carrier column, whose field's properties
     * are the leaves it carries; {@code evaluator} reads that column from a parent page.
     */
    public static Factory factory(ExpressionEvaluator.Factory evaluator, Attribute carrier, List<String> relativePaths) {
        if (carrier instanceof FieldAttribute == false) {
            throw new IllegalStateException("a collection's carrier must be a field, not [" + carrier + "]");
        }
        Map<String, EsField> properties = ((FieldAttribute) carrier).field().getProperties();
        List<String> leaves = properties.keySet().stream().sorted().toList();
        ElementType[] leafTypes = new ElementType[leaves.size()];
        for (int l = 0; l < leafTypes.length; l++) {
            leafTypes[l] = PlannerUtils.toElementType(properties.get(leaves.get(l)).getDataType());
        }
        int[] wanted = new int[relativePaths.size()];
        for (int w = 0; w < wanted.length; w++) {
            wanted[w] = leaves.indexOf(relativePaths.get(w));
            if (wanted[w] < 0) {
                throw new IllegalStateException("collection [" + carrier.name() + "] carries no [" + relativePaths.get(w) + "]");
            }
        }
        return new Factory(evaluator, leafTypes, wanted);
    }

    private final DriverContext context;
    private final ExpressionEvaluator carrier;
    private final ElementType[] leafTypes;
    private final int[] wanted;

    private CarriedChildRows(DriverContext context, ExpressionEvaluator carrier, ElementType[] leafTypes, int[] wanted) {
        this.context = context;
        this.carrier = carrier;
        this.leafTypes = leafTypes;
        this.wanted = wanted;
    }

    @Override
    public void read(Page parents, IntPredicate included, IntPredicate keepReading, Consumer<Page> consumer) {
        try (BytesRefBlock packed = (BytesRefBlock) carrier.eval(parents)) {
            ChildRelationColumns.decode(
                packed,
                leafTypes,
                wanted,
                included,
                keepReading,
                context.blockFactory(),
                NestedChildren.PAGE_SIZE,
                consumer
            );
        }
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(carrier);
    }
}
