/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;

public class Substring extends EsqlScalarFunction implements OptionalArgument {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "Substring",
        Substring::new
    );

    private final Expression str, start, length;

    @FunctionInfo(
        returnType = "keyword",
        description = "Returns a substring of a string, specified by a start position and an optional length.",
        examples = {
            @Example(file = "docs", tag = "substring", description = "This example returns the first three characters of every last name:"),
            @Example(file = "docs", tag = "substringEnd", description = """
                A negative start position is interpreted as being relative to the end of the string.
                This example returns the last three characters of every last name:"""),
            @Example(file = "docs", tag = "substringRemainder", description = """
                If length is omitted, substring returns the remainder of the string.
                This example returns all characters except for the first:""") }
    )
    public Substring(
        Source source,
        @Param(
            name = "string",
            type = { "keyword", "text" },
            description = "String expression. If `null`, the function returns `null`."
        ) Expression str,
        @Param(name = "start", type = { "integer" }, description = "Start position.") Expression start,
        @Param(
            optional = true,
            name = "length",
            type = { "integer" },
            description = "Length of the substring from the start position. Optional; if omitted, all positions after `start` are returned."
        ) Expression length
    ) {
        super(source, length == null ? Arrays.asList(str, start) : Arrays.asList(str, start, length));
        this.str = str;
        this.start = start;
        this.length = length;
    }

    private Substring(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(str);
        out.writeNamedWriteable(start);
        out.writeOptionalNamedWriteable(length);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
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

        TypeResolution resolution = isString(str, sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        resolution = TypeResolutions.isType(start, dt -> dt == INTEGER, sourceText(), SECOND, "integer");

        if (resolution.unresolved()) {
            return resolution;
        }

        return length == null
            ? TypeResolution.TYPE_RESOLVED
            : TypeResolutions.isType(length, dt -> dt == INTEGER, sourceText(), THIRD, "integer");
    }

    @Override
    public boolean foldable() {
        return str.foldable() && start.foldable() && (length == null || length.foldable());
    }

    @Evaluator(extraName = "NoLength")
    static BytesRef process(BytesRef str, int start) {
        int length = str.length; // we just need a value at least the length of the string
        return process(str, start, length);

    }

    @Evaluator
    static BytesRef process(BytesRef str, int start, int length) {
        if (length < 0) {
            throw new IllegalArgumentException("Length parameter cannot be negative, found [" + length + "]");
        }
        if (str.length == 0) {
            return str;
        }
        int codePointCount = UnicodeUtil.codePointCount(str);
        int indexStart = indexStart(codePointCount, start);
        int indexEnd = Math.min(codePointCount, indexStart + length);
        String s = str.utf8ToString();
        return new BytesRef(s.substring(s.offsetByCodePoints(0, indexStart), s.offsetByCodePoints(0, indexEnd)));
    }

    private static int indexStart(int codePointCount, int start) {
        // esql is 1-based when it comes to string manipulation. We treat start = 0 and 1 the same
        // a negative value is relative to the end of the string
        int indexStart;
        if (start > 0) {
            indexStart = start - 1;
        } else if (start < 0) {
            indexStart = codePointCount + start; // start is negative, so this is a subtraction
        } else {
            indexStart = start; // start == 0
        }
        return Math.min(Math.max(0, indexStart), codePointCount); // sanitise string start index
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Substring(source(), newChildren.get(0), newChildren.get(1), length == null ? null : newChildren.get(2));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Substring::new, str, start, length);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var strFactory = toEvaluator.apply(str);
        var startFactory = toEvaluator.apply(start);
        if (length == null) {
            return new SubstringNoLengthEvaluator.Factory(source(), strFactory, startFactory);
        }
        var lengthFactory = toEvaluator.apply(length);
        return new SubstringEvaluator.Factory(source(), strFactory, startFactory, lengthFactory);
    }

    Expression str() {
        return str;
    }

    Expression start() {
        return start;
    }

    Expression length() {
        return length;
    }
}
