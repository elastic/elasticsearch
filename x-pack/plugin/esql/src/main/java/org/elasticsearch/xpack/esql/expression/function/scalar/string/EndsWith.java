/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.WildcardQuery;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

public class EndsWith extends EsqlScalarFunction implements TranslationAware.SingleValueTranslationAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "EndsWith", EndsWith::new);

    private final Expression str;
    private final Expression suffix;

    @FunctionInfo(
        returnType = "boolean",
        description = "Returns a boolean that indicates whether a keyword string ends with another string.",
        examples = @Example(file = "string", tag = "endsWith")
    )
    public EndsWith(
        Source source,
        @Param(
            name = "str",
            type = { "keyword", "text" },
            description = "String expression. If `null`, the function returns `null`."
        ) Expression str,
        @Param(
            name = "suffix",
            type = { "keyword", "text" },
            description = "String expression. If `null`, the function returns `null`."
        ) Expression suffix
    ) {
        super(source, Arrays.asList(str, suffix));
        this.str = str;
        this.suffix = suffix;
    }

    private EndsWith(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(str);
        out.writeNamedWriteable(suffix);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isString(str, sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }
        return isString(suffix, sourceText(), SECOND);
    }

    @Override
    public boolean foldable() {
        return str.foldable() && suffix.foldable();
    }

    @Evaluator
    static boolean process(BytesRef str, BytesRef suffix) {
        if (str.length < suffix.length) {
            return false;
        }
        return Arrays.equals(
            str.bytes,
            str.offset + str.length - suffix.length,
            str.offset + str.length,
            suffix.bytes,
            suffix.offset,
            suffix.offset + suffix.length
        );
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new EndsWith(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, EndsWith::new, str, suffix);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return new EndsWithEvaluator.Factory(source(), toEvaluator.apply(str), toEvaluator.apply(suffix));
    }

    @Override
    public Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        return pushdownPredicates.isPushableAttribute(str) && suffix.foldable() ? Translatable.YES : Translatable.NO;
    }

    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        LucenePushdownPredicates.checkIsPushableAttribute(str);
        var fieldName = handler.nameOf(str instanceof FieldAttribute fa ? fa.exactAttribute() : str);

        // TODO: Get the real FoldContext here
        var wildcardQuery = "*" + QueryParser.escape(BytesRefs.toString(suffix.fold(FoldContext.small())));

        return new WildcardQuery(source(), fieldName, wildcardQuery);
    }

    @Override
    public Expression singleValueField() {
        return str;
    }

    Expression str() {
        return str;
    }

    Expression suffix() {
        return suffix;
    }
}
