/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

/**
 * Reduce a multivalued string field to a single valued field by concatenating all values.
 */
public class MvConcat extends BinaryScalarFunction implements EvaluatorMapper {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "MvConcat", MvConcat::new);

    @FunctionInfo(
        returnType = "keyword",
        description = "Converts a multivalued string expression into a single valued column "
            + "containing the concatenation of all values separated by a delimiter.",
        examples = {
            @Example(file = "string", tag = "mv_concat"),
            @Example(
                description = "To concat non-string columns, call <<esql-to_string>> first:",
                file = "string",
                tag = "mv_concat-to_string"
            ) }
    )
    public MvConcat(
        Source source,
        @Param(name = "string", type = { "text", "keyword" }, description = "Multivalue expression.") Expression field,
        @Param(name = "delim", type = { "text", "keyword" }, description = "Delimiter.") Expression delim
    ) {
        super(source, field, delim);
    }

    private MvConcat(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isString(left(), sourceText(), TypeResolutions.ParamOrdinal.FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        return isString(right(), sourceText(), TypeResolutions.ParamOrdinal.SECOND);
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return new EvaluatorFactory(toEvaluator.apply(left()), toEvaluator.apply(right()));
    }

    @Override
    public Object fold() {
        return EvaluatorMapper.super.fold();
    }

    @Override
    protected BinaryScalarFunction replaceChildren(Expression newLeft, Expression newRight) {
        return new MvConcat(source(), newLeft, newRight);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvConcat::new, left(), right());
    }

    private record EvaluatorFactory(ExpressionEvaluator.Factory field, ExpressionEvaluator.Factory delim)
        implements
            ExpressionEvaluator.Factory {
        @Override
        public ExpressionEvaluator get(DriverContext context) {
            return new Evaluator(context, field.get(context), delim.get(context));
        }

        @Override
        public String toString() {
            return "MvConcat[field=" + field + ", delim=" + delim + "]";
        }
    }

    /**
     * Evaluator for {@link MvConcat}. Not generated and doesn't extend from
     * {@link AbstractMultivalueFunction.AbstractEvaluator} because it's just
     * too different from all the other mv operators:
     * <ul>
     *     <li>It takes an extra parameter - the delimiter</li>
     *     <li>That extra parameter makes it much more likely to be {@code null}</li>
     *     <li>The actual joining process needs init step per row - {@link BytesRefBuilder#clear()}</li>
     * </ul>
     */
    private static class Evaluator implements ExpressionEvaluator {
        private final DriverContext context;
        private final ExpressionEvaluator field;
        private final ExpressionEvaluator delim;

        Evaluator(DriverContext context, ExpressionEvaluator field, ExpressionEvaluator delim) {
            this.context = context;
            this.field = field;
            this.delim = delim;
        }

        @Override
        public final Block eval(Page page) {
            try (BytesRefBlock fieldVal = (BytesRefBlock) field.eval(page); BytesRefBlock delimVal = (BytesRefBlock) delim.eval(page)) {
                int positionCount = page.getPositionCount();
                try (BytesRefBlock.Builder builder = context.blockFactory().newBytesRefBlockBuilder(positionCount)) {
                    BytesRefBuilder work = new BytesRefBuilder(); // TODO BreakingBytesRefBuilder so we don't blow past circuit breakers
                    BytesRef fieldScratch = new BytesRef();
                    BytesRef delimScratch = new BytesRef();
                    for (int p = 0; p < positionCount; p++) {
                        int fieldValueCount = fieldVal.getValueCount(p);
                        if (fieldValueCount == 0) {
                            builder.appendNull();
                            continue;
                        }
                        if (delimVal.getValueCount(p) != 1) {
                            builder.appendNull();
                            continue;
                        }
                        int first = fieldVal.getFirstValueIndex(p);
                        if (fieldValueCount == 1) {
                            builder.appendBytesRef(fieldVal.getBytesRef(first, fieldScratch));
                            continue;
                        }
                        int end = first + fieldValueCount;
                        BytesRef delim = delimVal.getBytesRef(delimVal.getFirstValueIndex(p), delimScratch);
                        work.clear();
                        work.append(fieldVal.getBytesRef(first, fieldScratch));
                        for (int i = first + 1; i < end; i++) {
                            work.append(delim);
                            work.append(fieldVal.getBytesRef(i, fieldScratch));
                        }
                        builder.appendBytesRef(work.get());
                    }
                    return builder.build();
                }
            }
        }

        @Override
        public final String toString() {
            return "MvConcat[field=" + field + ", delim=" + delim + "]";
        }

        @Override
        public void close() {}
    }
}
