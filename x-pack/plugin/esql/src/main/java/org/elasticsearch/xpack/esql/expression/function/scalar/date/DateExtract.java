/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.expression.function.scalar.ConfigurationFunction;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.session.Configuration;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isDate;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isStringAndExact;

public class DateExtract extends ConfigurationFunction implements EvaluatorMapper {

    private ChronoField chronoField;

    public DateExtract(Source source, Expression chronoFieldExp, Expression field, Configuration configuration) {
        super(source, List.of(chronoFieldExp, field), configuration);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        var fieldEvaluator = toEvaluator.apply(children().get(1));
        if (children().get(0).foldable()) {
            ChronoField chrono = chronoField();
            if (chrono == null) {
                BytesRef field = (BytesRef) children().get(0).fold();
                throw new EsqlIllegalArgumentException("invalid date field for [{}]: {}", sourceText(), field.utf8ToString());
            }
            return new DateExtractConstantEvaluator.Factory(fieldEvaluator, chrono, configuration().zoneId());
        }
        var chronoEvaluator = toEvaluator.apply(children().get(0));
        return new DateExtractEvaluator.Factory(source(), fieldEvaluator, chronoEvaluator, configuration().zoneId());
    }

    private ChronoField chronoField() {
        if (chronoField == null) {
            Expression field = children().get(0);
            if (field.foldable() && field.dataType() == DataTypes.KEYWORD) {
                try {
                    BytesRef br = BytesRefs.toBytesRef(field.fold());
                    chronoField = ChronoField.valueOf(br.utf8ToString().toUpperCase(Locale.ROOT));
                } catch (Exception e) {
                    return null;
                }
            }
        }
        return chronoField;
    }

    @Evaluator(warnExceptions = { IllegalArgumentException.class })
    static long process(long value, BytesRef chronoField, @Fixed ZoneId zone) {
        ChronoField chrono = ChronoField.valueOf(chronoField.utf8ToString().toUpperCase(Locale.ROOT));
        return Instant.ofEpochMilli(value).atZone(zone).getLong(chrono);
    }

    @Evaluator(extraName = "Constant")
    static long process(long value, @Fixed ChronoField chronoField, @Fixed ZoneId zone) {
        return Instant.ofEpochMilli(value).atZone(zone).getLong(chronoField);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new DateExtract(source(), newChildren.get(0), newChildren.get(1), configuration());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, DateExtract::new, children().get(0), children().get(1), configuration());
    }

    @Override
    public DataType dataType() {
        return DataTypes.LONG;
    }

    @Override
    public ScriptTemplate asScript() {
        throw new UnsupportedOperationException("functions do not support scripting");
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        return isStringAndExact(children().get(0), sourceText(), TypeResolutions.ParamOrdinal.FIRST).and(
            isDate(children().get(1), sourceText(), TypeResolutions.ParamOrdinal.SECOND)
        );
    }

    @Override
    public boolean foldable() {
        return children().get(0).foldable() && children().get(1).foldable();
    }

    @Override
    public Object fold() {
        return EvaluatorMapper.super.fold();
    }

}
