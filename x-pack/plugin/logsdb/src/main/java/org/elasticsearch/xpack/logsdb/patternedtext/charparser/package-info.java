/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * This package contains the core classes and interfaces for an efficient text parser that extracts patterns from text lines, typically
 * from log files. Each line is parsed into a sequence of tokens and subTokens, which are then processed based on a predefined schema.
 * The eventual output is an ordered list of {@link org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.Argument Argument}
 * instances, corresponding to the pattern parameters (including the timestamp), along with their types and positions in the original
 * text.
 * This output encapsulates all required information in order to reconstruct the template if needed, as well as the corresponding
 * parameter values and their offsets.
 *
 * <p><strong>Key Concepts</strong><br>
 * The heart of the system is the {@code schema.yaml} file, which contains a hierarchical definition of patterns to extract, including:
 * <ul>
 *     <li><strong>Tokens</strong> - Space/tab delimited elements in a line of text</li>
 *     <li><strong>SubTokens</strong> - Smaller components within tokens, separated by characters like periods, colons, etc.</li>
 *     <li><strong>MultiTokens</strong> - Sequences of tokens that together form a larger pattern</li>
 * </ul>
 *
 * The parsing workflow involves:
 * <ol>
 *     <li>Loading and compiling the schema into efficient data structures</li>
 *     <li>Analyzing input text character-by-character in a single pass</li>
 *     <li>Identifying tokens and their potential matches against defined patterns</li>
 *     <li>Extracting relevant information based on successful pattern matches into pattern arguments</li>
 * </ol>
 *
 * The core of the parser's efficiency comes from its use of bitmasks. Here's how it works:
 * <ul>
 *     <li>Bit Allocation: During the compilation phase, each sub-token, token, and multi-token type is assigned a unique bit in a 32-bit
 *     integer.</li>
 *     <li>Stateful Parsing: The parser processes the input string character by character. At each step, it maintains a set of bitmasks
 *     that represent the possible types for the current sub-token, token, and multi-token being parsed.</li>
 *     <li>Elimination: As the parser consumes more characters, it eliminates possibilities by performing bitwise AND operations on the
 *     bitmasks. For example, if the parser encounters a non-numeric character, it will clear the bits corresponding to all integer-based
 *     sub-token types.</li>
 *     <li>Type Identification: When a delimiter is reached, the parser finalizes the type of the preceding token or sub-token by
 *     identifying the highest-priority bit that is still set in the corresponding bitmask.</li>
 * </ul>
 *
 * <p><strong>Performance Principles</strong>
 * <p>The parsing schema is compiled into data structures that facilitate efficient parsing, according to the following principles:
 * <ul>
 *     <li>Ensure linear complexity by enforcing a single character-by-character pass. We may maintain as many parsing states as required
 *     for detecting template parameters, as long as we avoid backtracking and similar complexity-increasing operations that are used by
 *     regular expression engines and the like.</li>
 *     <li>Moreover, complexity should remain linear and additional overhead should be negligible when adding more patterns to detect
 *     (i.e., when extending the schema).</li>
 *     <li>Execute minimal and inexpensive operations for most parsed characters and execute heavier computations as rarely as
 *     possible.</li>
 *     <li>The former principle can be achieved by eliminating potential matches as early as possible and applying more expensive operations
 *     only on specific characters (e.g., subToken/token delimiters) and only if they are still required (meaning - only if not all
 *     options for match were already eliminated).</li>
 *     <li>Have bias towards using inexpensive computations like bitwise operations or simple calculations. For example, bitmasks provide
 *     manipulation of multiple states with a single inexpensive bitwise operation.</li>
 *     <li>Use fast access structures (like arrays) for in-parsing lookups and prefer cache-friendly structures
 *     (like primitive arrays).</li>
 *     <li>Use JIT-friendly concepts, like immutable and final classes and short methods (to favor fast inlining).</li>
 *     <li>Avoid allocations as much as possible in the parsing loop.</li>
 *     <li>Reduce method calls to a minimum. For example, with careful design of the parsing loop it is possible to only collect
 *     "decisions" and apply them at the end of the loop, thus avoiding method calls, while still not duplicating code.</li>
 * </ul>
 *
 * <p><strong>Usage</strong>
 * <p>The main entry point for using the parser is the
 * {@link org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.ParserFactory ParserFactory} class.
 * The factory provides a static method to create a new parser instance.
 * The parser can then be used to parse text lines and return an ordered list of typed arguments. Each argument includes its type,
 * extracted value, and its position within the original text, allowing the caller to construct the template if needed.
 * A reference implementation for constructing the template from the original text and the list of arguments is provided by the
 * {@link org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.Parser#constructPattern Parser.constructPattern} static method.
 *
 * <pre>{@code
 * import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.Parser;
 * import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.ParserFactory;
 * import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.PatternedMessage;
 *
 * public class ParserExample {
 *     public static void main(String[] args) {
 *         Parser parser = ParserFactory.createParser();
 *         String logLine = "2023-10-05 14:30:25 INFO received 305 packets from 135.122.123.222";
 *         List<Argument<?>> = parser.parse(logLine);
 *         StringBuilder pattern = new StringBuilder();
 *         Parser.constructPattern(logLine, arguments, pattern, true);
 *         System.out.println(pattern.toString()); // Outputs: "%T INFO received %N packets from %4"
 *     }
 * }
 * }</pre>
 *
 */
package org.elasticsearch.xpack.logsdb.patternedtext.charparser;
