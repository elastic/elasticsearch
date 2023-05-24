/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.xpack.esql.planner.Mappable;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.ql.type.DataTypes.BOOLEAN;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.IP;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.elasticsearch.xpack.ql.util.DateUtils.UTC_DATE_TIME_FORMATTER;

public class ToString extends AbstractConvertFunction implements Mappable {

    private static final String[] SUPPORTED_TYPE_NAMES = { "boolean", "datetime", "ip", "numerical", "string" };

    public ToString(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected Supplier<EvalOperator.ExpressionEvaluator> evaluator(Supplier<EvalOperator.ExpressionEvaluator> fieldEval) {
        DataType sourceType = field().dataType();

        if (sourceType == KEYWORD) {
            return fieldEval;
        } else if (sourceType == BOOLEAN) {
            return () -> new ToStringFromBooleanEvaluator(fieldEval.get());
        } else if (sourceType == DATETIME) {
            return () -> new ToStringFromDatetimeEvaluator(fieldEval.get());
        } else if (sourceType == IP) {
            return () -> new ToStringFromIPEvaluator(fieldEval.get());
        } else if (sourceType.isNumeric()) {
            if (sourceType.isRational()) {
                return () -> new ToStringFromDoubleEvaluator(fieldEval.get());
            } else if (sourceType == LONG) {
                return () -> new ToStringFromLongEvaluator(fieldEval.get());
            } else {
                return () -> new ToStringFromIntEvaluator(fieldEval.get());
            }
        }

        throw new AssertionError("unsupported type [" + sourceType + "]");
    }

    @Override
    public DataType dataType() {
        return KEYWORD;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToString(source(), newChildren.get(0));
    }

    @Override
    protected TypeResolution resolveFieldType() {
        return isType(field(), ToString::isTypeSupported, sourceText(), null, SUPPORTED_TYPE_NAMES);
    }

    private static boolean isTypeSupported(DataType dt) {
        return EsqlDataTypes.isString(dt) || dt == BOOLEAN || DataTypes.isDateTime(dt) || dt == IP || dt.isNumeric();
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToString::new, field());
    }

    @ConvertEvaluator(extraName = "FromBoolean")
    static BytesRef fromBoolean(boolean bool) {
        return new BytesRef(String.valueOf(bool));
    }

    @ConvertEvaluator(extraName = "FromIP")
    static BytesRef fromIP(BytesRef ip) {
        return new BytesRef(DocValueFormat.IP.format(ip));
    }

    @ConvertEvaluator(extraName = "FromDatetime")
    static BytesRef fromDatetime(long datetime) {
        return new BytesRef(UTC_DATE_TIME_FORMATTER.formatMillis(datetime));
    }

    @ConvertEvaluator(extraName = "FromDouble")
    static BytesRef fromDouble(double dbl) {
        return new BytesRef(String.valueOf(dbl));
    }

    @ConvertEvaluator(extraName = "FromLong")
    static BytesRef fromDouble(long lng) {
        return new BytesRef(String.valueOf(lng));
    }

    @ConvertEvaluator(extraName = "FromInt")
    static BytesRef fromDouble(int integer) {
        return new BytesRef(String.valueOf(integer));
    }
}
