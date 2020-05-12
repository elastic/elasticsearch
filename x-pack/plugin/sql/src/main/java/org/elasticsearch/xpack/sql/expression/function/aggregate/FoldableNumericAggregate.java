/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypeConverter;
import org.elasticsearch.xpack.ql.util.Check;

import java.util.List;

public abstract class FoldableNumericAggregate extends NumericAggregate {

    FoldableNumericAggregate(Source source, Expression field, List<Expression> parameters) {
        super(source, field, parameters);
    }

    FoldableNumericAggregate(Source source, Expression field) {
        super(source, field);
    }

    public boolean localFoldable() {
        return field().foldable();
    }

    public Object foldLocal() {
        Check.isTrue(field().foldable(), "argument of {} should be foldable", functionName());
        return DataTypeConverter.convert(field().fold(), dataType());
    }
}
