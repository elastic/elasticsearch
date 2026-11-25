/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.List;

/**
 * Decides when a conversion function resolves although the type of its input is not one it converts. Such a conversion is {@code null}
 * for every row, instead of failing the query: the other values of the query are not affected by a field that can't be converted.
 */
final class LenientConversion {
    private LenientConversion() {}

    /**
     * The resolution of a conversion function: its {@link ConvertFunction#strictTypeResolution() strict resolution}, or a resolved one
     * when the only thing wrong with it is the type of its input, and that type is one the function is lenient about.
     * <p>
     * It checks that by resolving the function again with the input replaced by {@code null}, which every conversion accepts. This
     * way, the other arguments of the function (options, base) are checked by the function itself, and an invalid one still fails, with
     * the strict error. Fields mapped to several types and fields of an unsupported type are not eligible here, as the analyzer
     * resolves them, and neither are types that can't be returned by a query: the strict error is returned for all of them.
     * </p>
     */
    static <T extends Expression & ConvertFunction> Expression.TypeResolution resolve(T convert) {
        Expression.TypeResolution strict = convert.strictTypeResolution();
        if (strict.resolved() || convert.lenientOnInputType() == false || convert.childrenResolved() == false) {
            return strict;
        }
        DataType inputType = convert.field().dataType();
        // An unsupported input is lenient only if it isn't a field: fields are unsupported, or mapped to several types, because of their
        // mapping, and the analyzer decides what to do with them. Anything else, like an ENRICH column, is a value of a type that
        // can't be converted.
        if ((inputType == DataType.UNSUPPORTED && convert.field() instanceof FieldAttribute)
            || DataType.isRepresentableInOutput(inputType) == false) {
            return strict;
        }
        List<Expression> children = new ArrayList<>(convert.children());
        children.set(0, Literal.NULL);
        Expression withoutInput = convert.replaceChildren(children);
        if (withoutInput instanceof ConvertFunction other && other.strictTypeResolution().resolved()) {
            return Expression.TypeResolution.TYPE_RESOLVED;
        }
        return strict;
    }
}
