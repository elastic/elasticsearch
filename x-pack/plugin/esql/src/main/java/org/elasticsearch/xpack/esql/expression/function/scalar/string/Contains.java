/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.WildcardQuery;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
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

/**
 * Contains function, given a string 'a' and a substring 'b', returns true if the substring 'b' is in 'a'.
 */
public class Contains extends EsqlScalarFunction implements OptionalArgument, TranslationAware.SingleValueTranslationAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Contains", Contains::new);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(Contains.class).binary(Contains::new).name("contains");

    /**
     * Gate for rewriting {@code LIKE "*literal*"} into {@link Contains} (see {@code ReplaceRegexMatch}). Only safe when every node
     * in the query — including remote clusters — both knows the {@code Contains} {@link #ENTRY} and implements
     * {@link TranslationAware} on it. Older nodes either lack {@code Contains} entirely (8.19) or carry a non-{@code TranslationAware}
     * {@code Contains} (9.3/9.4) that silently drops the {@code _index} pushdown semantics, so the rewrite must stay version-gated.
     */
    public static final TransportVersion LIKE_TO_CONTAINS_VERSION = TransportVersion.fromName("esql_like_to_contains");

    private final Expression str;
    private final Expression substr;

    @FunctionInfo(
        returnType = "boolean",
        briefSummary = "Checks whether a keyword substring is contained within another string.",
        description = """
            Returns a boolean that indicates whether a keyword substring is within another string.
            Returns `null` if either parameter is null.""",
        examples = @Example(file = "string", tag = "contains"),
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.2.0") }
    )
    public Contains(
        Source source,
        @Param(
            name = "string",
            type = { "keyword", "text" },
            description = "String expression: input string to check against. If `null`, the function returns `null`."
        ) Expression str,
        @Param(
            name = "substring",
            type = { "keyword", "text" },
            description = "String expression: A substring to find in the input string. If `null`, the function returns `null`."
        ) Expression substr
    ) {
        super(source, Arrays.asList(str, substr));
        this.str = str;
        this.substr = substr;
    }

    private Contains(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(str);
        out.writeNamedWriteable(substr);
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
        resolution = isString(substr, sourceText(), SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public boolean foldable() {
        return str.foldable() && substr.foldable();
    }

    @Evaluator
    static boolean process(BytesRef str, BytesRef substr) {
        if (str.length < substr.length) {
            return false;
        }
        return str.utf8ToString().contains(substr.utf8ToString());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Contains(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Contains::new, str, substr);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        ExpressionEvaluator.Factory strExpr = toEvaluator.apply(str);
        ExpressionEvaluator.Factory substrExpr = toEvaluator.apply(substr);

        return new ContainsEvaluator.Factory(source(), strExpr, substrExpr);
    }

    @Override
    public Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        return pushdownPredicates.isPushableAttribute(str) && substr.foldable() ? Translatable.YES : Translatable.NO;
    }

    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        LucenePushdownPredicates.checkIsPushableAttribute(str);
        var fieldName = handler.nameOf(str instanceof FieldAttribute fa ? fa.exactAttribute() : str);

        // TODO: Get the real FoldContext here
        var wildcardQuery = "*" + StringUtils.escapeWildcardLiteral(BytesRefs.toString(substr.fold(FoldContext.small()))) + "*";

        return new WildcardQuery(source(), fieldName, wildcardQuery, false, pushdownPredicates.flags().stringLikeOnIndex());
    }

    @Override
    public Expression singleValueField() {
        return str;
    }

    public Expression str() {
        return str;
    }

    public Expression substr() {
        return substr;
    }
}
