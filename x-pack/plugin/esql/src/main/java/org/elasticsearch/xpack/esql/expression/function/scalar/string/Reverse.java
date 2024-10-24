/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.common.util.ArrayUtils.reverseArray;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

/**
 * Function that reverses a string.
 */
public class Reverse extends UnaryScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Reverse", Reverse::new);

    @FunctionInfo(
        returnType = { "keyword" },
        description = "Returns a new string representing the input string in reverse order.",
        examples = {
            @Example(file = "string", tag = "reverse"),
            @Example(
                file = "string",
                tag = "reverseEmoji",
                description = "`REVERSE` works with unicode, too! It keeps unicode grapheme clusters together during reversal."
            ) }
    )
    public Reverse(
        Source source,
        @Param(
            name = "str",
            type = { "keyword", "text" },
            description = "String expression. If `null`, the function returns `null`."
        ) Expression field
    ) {
        super(source, field);
    }

    private Reverse(StreamInput in) throws IOException {
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

        return isString(field, sourceText(), DEFAULT);
    }

    /**
     * Reverses a unicode string, keeping grapheme clusters together
     */
    public static String reverseStringWithUnicodeCharacters(String str) {
        BreakIterator boundary = BreakIterator.getCharacterInstance(Locale.ROOT);
        boundary.setText(str);

        List<String> characters = new ArrayList<>();
        int start = boundary.first();
        for (int end = boundary.next(); end != BreakIterator.DONE; start = end, end = boundary.next()) {
            characters.add(str.substring(start, end));
        }

        StringBuilder reversed = new StringBuilder(str.length());
        for (int i = characters.size() - 1; i >= 0; i--) {
            reversed.append(characters.get(i));
        }

        return reversed.toString();
    }

    private static boolean reverseBytesIsReverseUnicode(BytesRef ref) {
        int end = ref.offset + ref.length;
        for (int i = ref.offset; i < end; i++) {
            if (ref.bytes[i] < 0 // Anything encoded in multibyte utf-8
                || ref.bytes[i] == 0x28 // Backspace
            ) {
                return false;
            }
        }
        return true;
    }

    @Evaluator
    static BytesRef process(BytesRef val) {
        if (reverseBytesIsReverseUnicode(val)) {
            // this is the fast path. we know we can just reverse the bytes.
            BytesRef reversed = BytesRef.deepCopyOf(val);
            reverseArray(reversed.bytes, reversed.offset, reversed.length);
            return reversed;
        }
        return new BytesRef(reverseStringWithUnicodeCharacters(val.utf8ToString()));
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var fieldEvaluator = toEvaluator.apply(field);
        return new ReverseEvaluator.Factory(source(), fieldEvaluator);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        assert newChildren.size() == 1;
        return new Reverse(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Reverse::new, field);
    }
}
