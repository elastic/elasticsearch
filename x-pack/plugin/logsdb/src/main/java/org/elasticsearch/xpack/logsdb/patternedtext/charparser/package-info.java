/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * This package contains the core classes and interfaces for an efficient text parser that extracts patterns from text lines, typically
 * from log files. Each line is parsed into a sequence of tokens and subTokens, which are then processed based on a predefined schema.
 * The eventual output contains the extracted timestamp (if exists), a template and template-parameters (if exist).
 * The parsing schema is compiled into data structures that facilitate efficient parsing, according to the following principles:
 * <ul>
 *     <li>Ensure linear complexity by enforcing a single character-by-character pass. We may maintain as many parsing states as required
 *     for detecting template parameters, as long as we avoid backtracking and similar complexity-increasing operations that are used by
 *     regular expression engines and the like.</li>
 *     <li>Moreover, complexity should remain linear and additional overhead should be negligible when adding more patterns to detect.</li>
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
 *     <li>Reduce method calls to a minimum. For example, direct use of a char array is ~20X faster than using
 *     {@link java.lang.StringBuilder}, although the latter has its obvious benefits.</li>
 * </ul>
 */
package org.elasticsearch.xpack.logsdb.patternedtext.charparser;
