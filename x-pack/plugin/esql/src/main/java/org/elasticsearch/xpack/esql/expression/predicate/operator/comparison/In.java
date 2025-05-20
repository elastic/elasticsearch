/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.TypedAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.Comparisons;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.TermQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.TermsQuery;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cast;
import org.elasticsearch.xpack.esql.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.core.expression.Foldables.valueOf;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_NANOS;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;
import static org.elasticsearch.xpack.esql.core.type.DataType.VERSION;
import static org.elasticsearch.xpack.esql.core.util.StringUtils.ordinal;

/**
 * The {@code IN} operator.
 * <p>
 *     This function has quite "unique" null handling rules around {@code null} and multivalued
 *     fields. The {@code null} rules are inspired by PostgreSQL, and, presumably, every other
 *     SQL implementation. The multivalue rules are pretty much an extension of the "multivalued
 *     fields are like null in scalars" rule. Here's some examples:
 * </p>
 * <ul>
 *     <li>{@code 'x' IN ('a', 'b', 'c')} => @{code false}</li>
 *     <li>{@code 'x' IN ('a', 'x', 'c')} => @{code true}</li>
 *     <li>{@code null IN ('a', 'b', 'c')} => @{code null}</li>
 *     <li>{@code ['x', 'y'] IN ('a', 'b', 'c')} => @{code null} and a warning</li>
 *     <li>{@code 'x' IN ('a', null, 'c')} => @{code null}</li>
 *     <li>{@code 'x' IN ('x', null, 'c')} => @{code true}</li>
 *     <li>{@code 'x' IN ('x', ['a', 'b'], 'c')} => @{code true} and a warning</li>
 *     <li>{@code 'x' IN ('a', ['a', 'b'], 'c')} => @{code false} and a warning</li>
 * </ul>
 * <p>
 *     And here's the decision tree for {@code WHERE x IN (a, b, c)}:
 * </p>
 * <ol>
 *     <li>{@code x IS NULL} => return {@code null}</li>
 *     <li>{@code MV_COUNT(x) > 1} => emit a warning and return {@code null}</li>
 *     <li>{@code a IS NULL AND b IS NULL AND c IS NULL} => return {@code null}</li>
 *     <li>{@code MV_COUNT(a) > 1 OR MV_COUNT(b) > 1 OR MV_COUNT(c) > 1} => emit a warning and continue</li>
 *     <li>{@code MV_COUNT(a) > 1 AND MV_COUNT(b) > 1 AND MV_COUNT(c) > 1} => return {@code null}</li>
 *     <li>{@code x == a OR x == b OR x == c} => return {@code true}</li>
 *     <li>{@code a IS NULL OR b IS NULL OR c IS NULL} => return {@code null}</li>
 *     <li>{@code else} => {@code false}</li>
 * </ol>
 * <p>
 *     I believe the first five entries are *mostly* optimizations and making the
 *     <a href="https://en.wikipedia.org/wiki/Three-valued_logic">Three-valued logic</a> of SQL
 *     explicit and integrated with our multivalue field rules. And make all that work with the
 *     actual evaluator code. You could probably shorten this to the last three points, but lots
 *     of folks aren't familiar with SQL's three-valued logic anyway, so let's be explicit.
 * </p>
 * <p>
 *     Because of this chain of logic we don't use the standard evaluator generators. They'd just
 *     require too many special cases and nothing else quite works like this. I mean, everything
 *     works just like this in that "three-valued logic" sort of way, but not in the "java code"
 *     sort of way. So! Instead of using the standard evaluator generators we use the
 *     String Template generators that we use for things like {@link Block} and {@link Vector}.
 * </p>
 */
public class In extends EsqlScalarFunction implements TranslationAware.SingleValueTranslationAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "In", In::new);
    private static final Logger logger = LogManager.getLogger(In.class);

    private final Expression value;
    private final List<Expression> list;

    @FunctionInfo(
        operator = "IN",
        returnType = "boolean",
        description = "The `IN` operator allows testing whether a field or expression equals an element in a list of literals, "
            + "fields or expressions.",
        examples = @Example(file = "row", tag = "in-with-expressions")
    )
    public In(
        Source source,
        @Param(
            name = "field",
            type = {
                "boolean",
                "cartesian_point",
                "cartesian_shape",
                "double",
                "geo_point",
                "geo_shape",
                "integer",
                "ip",
                "keyword",
                "long",
                "text",
                "version" },
            description = "An expression."
        ) Expression value,
        @Param(
            name = "inlist",
            type = {
                "boolean",
                "cartesian_point",
                "cartesian_shape",
                "double",
                "geo_point",
                "geo_shape",
                "integer",
                "ip",
                "keyword",
                "long",
                "text",
                "version" },
            description = "A list of items."
        ) List<Expression> list
    ) {
        super(source, CollectionUtils.combine(list, value));
        this.value = value;
        this.list = list;
    }

    public Expression value() {
        return value;
    }

    public List<Expression> list() {
        return list;
    }

    @Override
    public DataType dataType() {
        return BOOLEAN;
    }

    private In(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(value);
        out.writeNamedWriteableCollection(list);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, In::new, value, list);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new In(source(), newChildren.get(newChildren.size() - 1), newChildren.subList(0, newChildren.size() - 1));
    }

    @Override
    public boolean foldable() {
        // QL's In fold()s to null, if value() is null, but isn't foldable() unless all children are
        // TODO: update this null check in QL too?
        return Expressions.isGuaranteedNull(value)
            || Expressions.foldable(children())
            || (Expressions.foldable(list) && list.stream().allMatch(Expressions::isGuaranteedNull));
    }

    @Override
    public Object fold(FoldContext ctx) {
        if (Expressions.isGuaranteedNull(value) || list.stream().allMatch(Expressions::isGuaranteedNull)) {
            return null;
        }
        return super.fold(ctx);
    }

    protected boolean areCompatible(DataType left, DataType right) {
        if (left == UNSIGNED_LONG || right == UNSIGNED_LONG) {
            // automatic numerical conversions not applicable for UNSIGNED_LONG, see Verifier#validateUnsignedLongOperator().
            return left == right;
        }
        if (DataType.isSpatial(left) && DataType.isSpatial(right)) {
            return left == right;
        }
        return DataType.areCompatible(left, right);
    }

    @Override
    protected TypeResolution resolveType() { // TODO: move the foldability check from QL's In to SQL's and remove this method
        TypeResolution resolution = EsqlTypeResolutions.isExact(value, functionName(), DEFAULT);
        if (resolution.unresolved()) {
            return resolution;
        }

        DataType dt = value.dataType();
        if (dt.isDate()) {
            // If value is a date (nanos or millis), list cannot contain both nanos and millis
            DataType seenDateType = null;
            for (int i = 0; i < list.size(); i++) {
                Expression listValue = list.get(i);
                if (seenDateType == null && listValue.dataType().isDate()) {
                    seenDateType = listValue.dataType();
                }
                // The areCompatible test is still necessary to account for nulls.
                if ((listValue.dataType().isDate() && listValue.dataType() != seenDateType)
                    || (listValue.dataType().isDate() == false && areCompatible(dt, listValue.dataType()) == false)) {
                    return new TypeResolution(
                        format(
                            null,
                            "{} argument of [{}] must be [{}], found value [{}] type [{}]",
                            ordinal(i + 1),
                            sourceText(),
                            dt.typeName(),
                            Expressions.name(listValue),
                            listValue.dataType().typeName()
                        )
                    );
                }
            }

        } else {
            for (int i = 0; i < list.size(); i++) {
                Expression listValue = list.get(i);
                if (areCompatible(dt, listValue.dataType()) == false) {
                    return new TypeResolution(
                        format(
                            null,
                            "{} argument of [{}] must be [{}], found value [{}] type [{}]",
                            ordinal(i + 1),
                            sourceText(),
                            dt.typeName(),
                            Expressions.name(listValue),
                            listValue.dataType().typeName()
                        )
                    );
                }
            }
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    protected Expression canonicalize() {
        // order values for commutative operators
        List<Expression> canonicalValues = Expressions.canonicalize(list);
        Collections.sort(canonicalValues, (l, r) -> Integer.compare(l.hashCode(), r.hashCode()));
        return new In(source(), value, canonicalValues);
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        EvalOperator.ExpressionEvaluator.Factory lhs;
        EvalOperator.ExpressionEvaluator.Factory[] factories;
        if (value.dataType() == DATE_NANOS && list.get(0).dataType() == DATETIME) {
            lhs = toEvaluator.apply(value);
            factories = list.stream().map(toEvaluator::apply).toArray(EvalOperator.ExpressionEvaluator.Factory[]::new);
            return new InNanosMillisEvaluator.Factory(source(), lhs, factories);
        }
        if (value.dataType() == DATETIME && list.get(0).dataType() == DATE_NANOS) {
            lhs = toEvaluator.apply(value);
            factories = list.stream().map(toEvaluator::apply).toArray(EvalOperator.ExpressionEvaluator.Factory[]::new);
            return new InMillisNanosEvaluator.Factory(source(), lhs, factories);
        }
        var commonType = commonType();
        if (commonType.isNumeric()) {
            lhs = Cast.cast(source(), value.dataType(), commonType, toEvaluator.apply(value));
            factories = list.stream()
                .map(e -> Cast.cast(source(), e.dataType(), commonType, toEvaluator.apply(e)))
                .toArray(EvalOperator.ExpressionEvaluator.Factory[]::new);
        } else {
            lhs = toEvaluator.apply(value);
            factories = list.stream().map(toEvaluator::apply).toArray(EvalOperator.ExpressionEvaluator.Factory[]::new);
        }

        if (commonType == BOOLEAN) {
            return new InBooleanEvaluator.Factory(source(), lhs, factories);
        }
        if (commonType == DOUBLE) {
            return new InDoubleEvaluator.Factory(source(), lhs, factories);
        }
        if (commonType == INTEGER) {
            return new InIntEvaluator.Factory(source(), lhs, factories);
        }
        if (commonType == LONG || commonType == DATETIME || commonType == DATE_NANOS || commonType == UNSIGNED_LONG) {
            return new InLongEvaluator.Factory(source(), lhs, factories);
        }
        if (commonType == KEYWORD
            || commonType == TEXT
            || commonType == IP
            || commonType == VERSION
            || commonType == UNSUPPORTED
            || DataType.isSpatial(commonType)) {
            return new InBytesRefEvaluator.Factory(source(), toEvaluator.apply(value), factories);
        }
        if (commonType == NULL) {
            return EvalOperator.CONSTANT_NULL_FACTORY;
        }
        throw EsqlIllegalArgumentException.illegalDataType(commonType);
    }

    private DataType commonType() {
        DataType commonType = value.dataType();
        for (Expression e : list) {
            if (e.dataType() == NULL && value.dataType() != NULL) {
                continue;
            }
            if (DataType.isSpatial(commonType)) {
                if (e.dataType() == commonType) {
                    continue;
                } else {
                    commonType = NULL;
                    break;
                }
            }
            commonType = EsqlDataTypeConverter.commonType(commonType, e.dataType());
        }
        return commonType;
    }

    static boolean process(BitSet nulls, BitSet mvs, int lhs, int[] rhs) {
        for (int i = 0; i < rhs.length; i++) {
            if ((nulls != null && nulls.get(i)) || (mvs != null && mvs.get(i))) {
                continue;
            }
            Boolean compResult = Comparisons.eq(lhs, rhs[i]);
            if (compResult == Boolean.TRUE) {
                return true;
            }
        }
        return false;
    }

    static boolean process(BitSet nulls, BitSet mvs, long lhs, long[] rhs) {
        for (int i = 0; i < rhs.length; i++) {
            if ((nulls != null && nulls.get(i)) || (mvs != null && mvs.get(i))) {
                continue;
            }
            Boolean compResult = Comparisons.eq(lhs, rhs[i]);
            if (compResult == Boolean.TRUE) {
                return true;
            }
        }
        return false;
    }

    /**
     * Processor for mixed millisecond and nanosecond dates, where the "value" (aka lhs) is in nanoseconds
     * and the "list" (aka rhs) is in milliseconds
     */
    static boolean processNanosMillis(BitSet nulls, BitSet mvs, long lhs, long[] rhs) {
        for (int i = 0; i < rhs.length; i++) {
            if ((nulls != null && nulls.get(i)) || (mvs != null && mvs.get(i))) {
                continue;
            }
            if (DateUtils.compareNanosToMillis(lhs, rhs[i]) == 0) {
                return true;
            }
        }
        return false;
    }

    /**
     *  Processor for mixed millisecond and nanosecond dates, where the "value" (aka lhs) is in milliseoncds
     *  and the "list" (aka rhs) is in nanoseconds
     */
    static boolean processMillisNanos(BitSet nulls, BitSet mvs, long lhs, long[] rhs) {
        for (int i = 0; i < rhs.length; i++) {
            if ((nulls != null && nulls.get(i)) || (mvs != null && mvs.get(i))) {
                continue;
            }
            Boolean compResult = DateUtils.compareNanosToMillis(rhs[i], lhs) == 0;
            if (compResult == Boolean.TRUE) {
                return true;
            }
        }
        return false;
    }

    static boolean process(BitSet nulls, BitSet mvs, double lhs, double[] rhs) {
        for (int i = 0; i < rhs.length; i++) {
            if ((nulls != null && nulls.get(i)) || (mvs != null && mvs.get(i))) {
                continue;
            }
            Boolean compResult = Comparisons.eq(lhs, rhs[i]);
            if (compResult == Boolean.TRUE) {
                return true;
            }
        }
        return false;
    }

    static boolean process(BitSet nulls, BitSet mvs, BytesRef lhs, BytesRef[] rhs) {
        for (int i = 0; i < rhs.length; i++) {
            if ((nulls != null && nulls.get(i)) || (mvs != null && mvs.get(i))) {
                continue;
            }
            Boolean compResult = Comparisons.eq(lhs, rhs[i]);
            if (compResult == Boolean.TRUE) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        return pushdownPredicates.isPushableAttribute(value) && Expressions.foldable(list()) ? Translatable.YES : Translatable.NO;
    }

    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        return translate(pushdownPredicates, handler);
    }

    private Query translate(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        logger.trace("Attempting to generate lucene query for IN expression");
        TypedAttribute attribute = LucenePushdownPredicates.checkIsPushableAttribute(value());

        Set<Object> terms = new LinkedHashSet<>();
        List<Query> queries = new ArrayList<>();

        for (Expression rhs : list()) {
            if (Expressions.isGuaranteedNull(rhs) == false) {
                if (needsTypeSpecificValueHandling(attribute.dataType())) {
                    // delegates to BinaryComparisons translator to ensure consistent handling of date and time values
                    // TODO:
                    // Query query = BinaryComparisons.translate(new Equals(in.source(), in.value(), rhs), handler);
                    Query query = handler.asQuery(pushdownPredicates, new Equals(source(), value(), rhs));

                    if (query instanceof TermQuery) {
                        terms.add(((TermQuery) query).value());
                    } else {
                        queries.add(query);
                    }
                } else {
                    terms.add(valueOf(FoldContext.small() /* TODO remove me */, rhs));
                }
            }
        }

        if (terms.isEmpty() == false) {
            String fieldName = LucenePushdownPredicates.pushableAttributeName(attribute);
            queries.add(new TermsQuery(source(), fieldName, terms));
        }

        return queries.stream().reduce((q1, q2) -> or(source(), q1, q2)).get();
    }

    private static boolean needsTypeSpecificValueHandling(DataType fieldType) {
        return fieldType == DATETIME || fieldType == DATE_NANOS || fieldType == IP || fieldType == VERSION || fieldType == UNSIGNED_LONG;
    }

    private static Query or(Source source, Query left, Query right) {
        return BinaryLogic.boolQuery(source, left, right, false);
    }

    @Override
    public Expression singleValueField() {
        return value;
    }
}
