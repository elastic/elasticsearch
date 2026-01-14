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
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.compute.ann.Fixed.Scope.THREAD_LOCAL;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

/**
 * A fun Easter egg function that wraps text in ASCII art of a chicken saying something,
 * similar to the classic "cowsay" command.
 */
public class Chicken extends UnaryScalarFunction {
    public static final String CHICKEN_EMOJI = "\uD83D\uDC14";

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Chicken", Chicken::new);

    private static final int DEFAULT_WIDTH = 40;
    private static final int MAX_WIDTH = 76;

    // Pre-allocated BytesRef constants for single characters to avoid allocations
    private static final BytesRef UNDERSCORE = new BytesRef("_");
    private static final BytesRef DASH = new BytesRef("-");
    private static final BytesRef SPACE = new BytesRef(" ");
    private static final BytesRef NEWLINE = new BytesRef("\n");
    private static final BytesRef OPEN_ANGLE = new BytesRef("< ");
    private static final BytesRef CLOSE_ANGLE = new BytesRef(" >");
    private static final BytesRef OPEN_SLASH = new BytesRef("/ ");
    private static final BytesRef CLOSE_BACKSLASH = new BytesRef(" \\");
    private static final BytesRef OPEN_BACKSLASH = new BytesRef("\\ ");
    private static final BytesRef CLOSE_SLASH = new BytesRef(" /");
    private static final BytesRef OPEN_PIPE = new BytesRef("| ");
    private static final BytesRef CLOSE_PIPE = new BytesRef(" |");

    // The chicken ASCII art (credit: cf) - pre-allocated as BytesRef
    private static final BytesRef CHICKEN_ART = new BytesRef("""
             \\
              \\__//
              /.__.\\.
              \\ \\/ /
           '__/    \\
            \\-      )
             \\_____/
          _____|_|____
               " "
        """);

    @FunctionInfo(returnType = "keyword", description = """
        Returns a string with the input text wrapped in ASCII art of a chicken saying the message.
        This is an Easter egg function inspired by the classic "cowsay" command.""")
    public Chicken(
        Source source,
        @Param(name = "message", type = { "keyword", "text" }, description = "The message for the chicken to say.") Expression message
    ) {
        super(source, message);
    }

    private Chicken(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field());
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

        return isString(field(), sourceText(), DEFAULT);
    }

    @Evaluator
    static BytesRef process(@Fixed(includeInToString = false, scope = THREAD_LOCAL) BreakingBytesRefBuilder scratch, BytesRef message) {
        String text = message.utf8ToString();
        scratch.clear();
        buildChickenSay(scratch, text, DEFAULT_WIDTH);
        return scratch.bytesRefView();
    }

    /**
     * Builds the complete chicken say output with speech bubble and ASCII art,
     * appending directly to the provided builder.
     */
    static void buildChickenSay(BreakingBytesRefBuilder scratch, String message, int maxWidth) {
        // Clamp width
        int width = Math.min(maxWidth, MAX_WIDTH);

        // Wrap the message into lines
        List<String> lines = wrapText(message, width);

        // Calculate the actual width needed
        int bubbleWidth = lines.stream().mapToInt(String::length).max().orElse(0);
        bubbleWidth = Math.max(bubbleWidth, 2); // Minimum width

        // Top border: " " + "_".repeat(bubbleWidth + 2) + "\n"
        scratch.append(SPACE);
        appendRepeated(scratch, UNDERSCORE, bubbleWidth + 2);
        scratch.append(NEWLINE);

        // Message lines
        if (lines.size() == 1) {
            // Single line: use < >
            scratch.append(OPEN_ANGLE);
            appendPadRight(scratch, lines.get(0), bubbleWidth);
            scratch.append(CLOSE_ANGLE);
            scratch.append(NEWLINE);
        } else {
            // Multi-line: use / \ for first/last, | | for middle
            for (int i = 0; i < lines.size(); i++) {
                String line = lines.get(i);
                if (i == 0) {
                    scratch.append(OPEN_SLASH);
                    appendPadRight(scratch, line, bubbleWidth);
                    scratch.append(CLOSE_BACKSLASH);
                } else if (i == lines.size() - 1) {
                    scratch.append(OPEN_BACKSLASH);
                    appendPadRight(scratch, line, bubbleWidth);
                    scratch.append(CLOSE_SLASH);
                } else {
                    scratch.append(OPEN_PIPE);
                    appendPadRight(scratch, line, bubbleWidth);
                    scratch.append(CLOSE_PIPE);
                }
                scratch.append(NEWLINE);
            }
        }

        // Bottom border: " " + "-".repeat(bubbleWidth + 2) + "\n"
        scratch.append(SPACE);
        appendRepeated(scratch, DASH, bubbleWidth + 2);
        scratch.append(NEWLINE);

        // Add the chicken
        scratch.append(CHICKEN_ART);
    }

    /**
     * Appends a BytesRef repeated n times to the builder.
     */
    private static void appendRepeated(BreakingBytesRefBuilder scratch, BytesRef ref, int count) {
        for (int i = 0; i < count; i++) {
            scratch.append(ref);
        }
    }

    /**
     * Appends a string to the builder, padded with spaces to reach the target width.
     */
    private static void appendPadRight(BreakingBytesRefBuilder scratch, String s, int width) {
        scratch.append(new BytesRef(s));
        int padding = width - s.length();
        if (padding > 0) {
            appendRepeated(scratch, SPACE, padding);
        }
    }

    /**
     * Wraps text to fit within the specified width.
     */
    static List<String> wrapText(String text, int width) {
        if (text == null || text.isEmpty()) {
            return List.of("");
        }

        List<String> lines = new java.util.ArrayList<>();
        String[] words = text.split(" ");
        StringBuilder currentLine = new StringBuilder();

        for (String word : words) {
            if (currentLine.isEmpty()) {
                currentLine.append(word);
            } else if (currentLine.length() + 1 + word.length() <= width) {
                currentLine.append(" ").append(word);
            } else {
                lines.add(currentLine.toString());
                currentLine = new StringBuilder(word);
            }
        }

        if (currentLine.isEmpty() == false) {
            lines.add(currentLine.toString());
        }

        return lines.isEmpty() ? List.of("") : lines;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Chicken(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Chicken::new, field());
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return new ChickenEvaluator.Factory(
            source(),
            context -> new BreakingBytesRefBuilder(context.breaker(), "chicken"),
            toEvaluator.apply(field())
        );
    }
}
