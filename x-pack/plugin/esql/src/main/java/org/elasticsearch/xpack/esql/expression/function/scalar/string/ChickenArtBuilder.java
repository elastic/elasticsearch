/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

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
              \\       ______
               \\     /______\\
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
    REALISTIC("""
             \\
              \\  .-=-.
                _/     `.
               /_( o    |.__
                 \\          ``-._
                   |    _...     `
                   |  .'    `-. _,'
                    \\ `-.    ,' ;
                     `.  `~~'  /
                     _ 7`"..."'
                     ,'| __/
                         ,'\
        """),
    // Whistling Chicken
    WHISTLING("""
             \\      |
              \\    MM
               \\  =' \\___/|
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

    private static final byte SPACE_BYTE = ' ';
    private static final int DEFAULT_WIDTH = 40;
    private static final int MAX_WIDTH = 76;

    final BytesRef ascii;

    ChickenArtBuilder(String ascii) {
        this.ascii = new BytesRef(ascii);
    }

    // Lookup map for style names (case-insensitive)
    private static final Map<String, ChickenArtBuilder> STYLE_MAP = java.util.Arrays.stream(values())
        .collect(Collectors.toMap(c -> c.name().toLowerCase(Locale.ROOT), c -> c));

    /**
     * Returns a comma-separated list of all available style names.
     */
    public static String availableStyles() {
        return java.util.Arrays.stream(values()).map(c -> c.name().toLowerCase(Locale.ROOT)).collect(Collectors.joining(", "));
    }

    /**
     * Returns a randomly selected chicken art builder.
     */
    public static ChickenArtBuilder random() {
        ChickenArtBuilder[] values = values();
        return values[Randomness.get().nextInt(values.length)];
    }

    /**
     * Looks up a chicken style by name (case-insensitive).
     * Returns null if the style is not found.
     */
    public static ChickenArtBuilder fromName(String name) {
        if (name == null) {
            return null;
        }
        return STYLE_MAP.get(name.toLowerCase(Locale.ROOT));
    }

    /**
     * Looks up a chicken style by name from a BytesRef (case-insensitive).
     * Returns null if the style is not found.
     */
    public static ChickenArtBuilder fromName(BytesRef name) {
        if (name == null) {
            return null;
        }
        return fromName(name.utf8ToString());
    }

    /**
     * Builds the complete chicken say output with speech bubble and ASCII art,
     * appending directly to the provided {@link BreakingBytesRefBuilder}.
     * Works directly with BytesRef to avoid String conversion.
     */
    public void buildChickenSay(BreakingBytesRefBuilder scratch, BytesRef message) {
        buildChickenSay(scratch, message, DEFAULT_WIDTH);
    }

    /**
     * Builds the complete chicken say output with speech bubble and ASCII art,
     * appending directly to the provided {@link BreakingBytesRefBuilder}.
     * Works directly with BytesRef to avoid String conversion.
     */
    public void buildChickenSay(BreakingBytesRefBuilder scratch, BytesRef message, int maxWidth) {
        // Clamp width
        int width = Math.min(maxWidth, MAX_WIDTH);

        // Wrap the message into lines (as byte ranges)
        List<LineRange> lines = wrapBytes(message, width);

        // Calculate the actual width needed (using display width)
        int bubbleWidth = 0;
        for (LineRange line : lines) {
            bubbleWidth = Math.max(bubbleWidth, line.displayWidth);
        }
        bubbleWidth = Math.max(bubbleWidth, 2); // Minimum width

        // Top border
        scratch.append(SPACE);
        appendRepeated(scratch, UNDERSCORE, bubbleWidth + 2);
        scratch.append(NEWLINE);

        // Message lines
        if (lines.size() == 1) {
            // Single line: use < >
            scratch.append(OPEN_ANGLE);
            appendLineWithPadding(scratch, message, lines.get(0), bubbleWidth);
            scratch.append(CLOSE_ANGLE);
            scratch.append(NEWLINE);
        } else {
            // Multi-line: use / \ for first/last, | | for middle
            for (int i = 0; i < lines.size(); i++) {
                LineRange line = lines.get(i);
                if (i == 0) {
                    scratch.append(OPEN_SLASH);
                    appendLineWithPadding(scratch, message, line, bubbleWidth);
                    scratch.append(CLOSE_BACKSLASH);
                } else if (i == lines.size() - 1) {
                    scratch.append(OPEN_BACKSLASH);
                    appendLineWithPadding(scratch, message, line, bubbleWidth);
                    scratch.append(CLOSE_SLASH);
                } else {
                    scratch.append(OPEN_PIPE);
                    appendLineWithPadding(scratch, message, line, bubbleWidth);
                    scratch.append(CLOSE_PIPE);
                }
                scratch.append(NEWLINE);
            }
        }

        // Bottom border
        scratch.append(SPACE);
        appendRepeated(scratch, DASH, bubbleWidth + 2);
        scratch.append(NEWLINE);

        // Add this chicken's ASCII art
        scratch.append(ascii);
    }

    /**
     * Represents a line as a byte range within the original message.
     */
    record LineRange(int startOffset, int endOffset, int displayWidth) {}

    /**
     * Wraps bytes to fit within the specified display width.
     * Returns a list of LineRange objects representing each wrapped line.
     */
    static List<LineRange> wrapBytes(BytesRef message, int maxDisplayWidth) {
        if (message.length == 0) {
            return List.of(new LineRange(message.offset, message.offset, 0));
        }

        List<LineRange> lines = new ArrayList<>();
        byte[] bytes = message.bytes;
        int offset = message.offset;
        int end = message.offset + message.length;

        int lineStart = offset;
        int lineDisplayWidth = 0;
        int wordStart = offset;
        int wordDisplayWidth = 0;

        int i = offset;
        while (i < end) {
            byte b = bytes[i];

            if (b == SPACE_BYTE) {
                // End of a word
                if (lineStart == wordStart) {
                    // First word on this line
                    lineDisplayWidth = wordDisplayWidth;
                } else if (lineDisplayWidth + 1 + wordDisplayWidth <= maxDisplayWidth) {
                    // Word fits on current line (with space)
                    lineDisplayWidth += 1 + wordDisplayWidth;
                } else {
                    // Word doesn't fit - emit current line and start new one
                    lines.add(new LineRange(lineStart, wordStart - 1, lineDisplayWidth));
                    lineStart = wordStart;
                    lineDisplayWidth = wordDisplayWidth;
                }
                wordStart = i + 1;
                wordDisplayWidth = 0;
                i++;
            } else {
                // Part of a word - count UTF-8 code point
                int codePointBytes = utf8CodePointLength(b);
                wordDisplayWidth++;
                i += codePointBytes;
            }
        }

        // Handle the last word
        if (wordStart < end) {
            if (lineStart == wordStart) {
                lineDisplayWidth = wordDisplayWidth;
            } else if (lineDisplayWidth + 1 + wordDisplayWidth <= maxDisplayWidth) {
                lineDisplayWidth += 1 + wordDisplayWidth;
            } else {
                lines.add(new LineRange(lineStart, wordStart - 1, lineDisplayWidth));
                lineStart = wordStart;
                lineDisplayWidth = wordDisplayWidth;
            }
        }

        // Emit the final line
        lines.add(new LineRange(lineStart, end, lineDisplayWidth));

        return lines.isEmpty() ? List.of(new LineRange(message.offset, message.offset, 0)) : lines;
    }

    /**
     * Returns the number of bytes in a UTF-8 code point based on the leading byte.
     */
    private static int utf8CodePointLength(byte leadingByte) {
        if ((leadingByte & 0x80) == 0) {
            return 1; // ASCII
        } else if ((leadingByte & 0xE0) == 0xC0) {
            return 2;
        } else if ((leadingByte & 0xF0) == 0xE0) {
            return 3;
        } else if ((leadingByte & 0xF8) == 0xF0) {
            return 4;
        }
        return 1; // Invalid UTF-8, treat as single byte
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
     * Appends a line from the message with padding to reach the target width.
     */
    private static void appendLineWithPadding(BreakingBytesRefBuilder scratch, BytesRef message, LineRange line, int targetWidth) {
        if (line.endOffset > line.startOffset) {
            scratch.append(message.bytes, line.startOffset, line.endOffset - line.startOffset);
        }
        int padding = targetWidth - line.displayWidth;
        if (padding > 0) {
            appendRepeated(scratch, SPACE, padding);
        }
    }
}
