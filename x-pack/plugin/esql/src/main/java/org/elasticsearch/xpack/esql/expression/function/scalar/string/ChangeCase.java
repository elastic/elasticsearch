/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlConfigurationFunction;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.List;
import java.util.Locale;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

public abstract class ChangeCase extends EsqlConfigurationFunction {

    public enum Case {
        UPPER {
            @Override
            String process(String value, Locale locale) {
                return value.toUpperCase(locale);
            }

            @Override
            public boolean matchesCase(String value) {
                return value.codePoints().allMatch(cp -> Character.getType(cp) != Character.LOWERCASE_LETTER);
            }
        },
        LOWER {
            @Override
            String process(String value, Locale locale) {
                return value.toLowerCase(locale);
            }

            @Override
            public boolean matchesCase(String value) {
                return value.codePoints().allMatch(cp -> Character.getType(cp) != Character.UPPERCASE_LETTER);
            }
        };

        abstract String process(String value, Locale locale);

        public abstract boolean matchesCase(String value);
    }

    private final Expression field;
    private final Case caseType;

    protected ChangeCase(Source source, Expression field, Configuration configuration, Case caseType) {
        super(source, List.of(field), configuration);
        this.field = field;
        this.caseType = caseType;
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        return isString(field, sourceText(), DEFAULT);
    }

    @Override
    public boolean foldable() {
        return field.foldable();
    }

    public Expression field() {
        return field;
    }

    public Case caseType() {
        return caseType;
    }

    public abstract Expression replaceChild(Expression child);

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        assert newChildren.size() == 1;
        return replaceChild(newChildren.get(0));
    }

    @ConvertEvaluator
    static BytesRef process(BytesRef val, @Fixed Locale locale, @Fixed Case caseType) {
        return BytesRefs.toBytesRef(caseType.process(val.utf8ToString(), locale));
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var fieldEvaluator = toEvaluator.apply(field);
        return new ChangeCaseEvaluator.Factory(source(), fieldEvaluator, configuration().locale(), caseType);
    }
}
