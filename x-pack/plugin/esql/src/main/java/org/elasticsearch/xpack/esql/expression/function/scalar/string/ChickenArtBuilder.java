/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Collection of chicken ASCII art styles with builder functionality.
 * Used by the {@link Chicken} Easter egg function.
 */
public enum ChickenArtBuilder {
    // Ordinary Chicken
    ORDINARY("""
             \\
              \\    MM
               \\ <' \\___/|
                  \\_  _/
                    ][
        """),

    // Chicken in an Early State (Egg)
    EARLY_STATE("""
             \\
              \\    _--_
               \\  /    \\
                 |      |
                  \\____/
        """),

    // Chicken Laying an Egg
    LAYING("""
             \\
              \\    MM
               \\ <' \\___/|
                  \\_  _/ O
                    ][
        """),

    // Chicken that Thinks it is a Duck
    THINKS_ITS_A_DUCK("""
             \\
              \\    MM
               \\  <' \\___/|
                    \\    /
               ~~~~~~~~~~~~~~~
        """),

    // Chicken Smoking a Pipe
    SMOKING_A_PIPE("""
             \\
              \\    MM
               \\ <' \\___/|
             u/  \\_  _/
                   ][
        """),

    // Chicken Soup
    SOUP("""
             \\
              \\   ______
               \\ /______\\
                \\___________/
        """),

    // Racing Chicken
    RACING("""
             \\
              \\      ///
               \\   <' \\___/|
                    \\_  _/
                      \\ \\
        """),

    // Stoned Chicken
    STONED("""
             \\
              \\    MM
               \\ <@ \\___/|
                  \\____/
                    ><
        """),

    // Whistling Chicken
    WHISTLING("""
             \\           |
              \\         MM
               \\       =' \\___/|
                        \\_  _/
                          ][
        """);

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

    private static final int DEFAULT_WIDTH = 40;
    private static final int MAX_WIDTH = 76;

    private final BytesRef ascii;

    ChickenArtBuilder(String ascii) {
        this.ascii = new BytesRef(ascii);
    }

    /**
     * Returns a randomly selected chicken art builder.
     */
    public static ChickenArtBuilder random() {
        ChickenArtBuilder[] values = values();
        return values[ThreadLocalRandom.current().nextInt(values.length)];
    }

    /**
     * Builds the complete chicken say output with speech bubble and ASCII art,
     * appending directly to the provided {@link BreakingBytesRefBuilder}.
     */
    public void buildChickenSay(BreakingBytesRefBuilder scratch, String message) {
        buildChickenSay(scratch, message, DEFAULT_WIDTH);
    }

    /**
     * Builds the complete chicken say output with speech bubble and ASCII art,
     * appending directly to the provided {@link BreakingBytesRefBuilder}.
     */
    public void buildChickenSay(BreakingBytesRefBuilder scratch, String message, int maxWidth) {
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

        // Add this chicken's ASCII art
        scratch.append(ascii);
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

        List<String> lines = new ArrayList<>();
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
}
