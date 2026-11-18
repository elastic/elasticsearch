/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression.predicate.regex;

import java.util.Locale;

/**
 * The syntax {@code RLIKE} reads its regular expressions in, which the {@code regex_syntax} query setting picks.
 */
public enum RegexSyntax {
    /** Lucene's regexp syntax, {@code RLIKE}'s own; see {@link RLikePattern}. */
    LUCENE,
    /** RE2 syntax, that of Go's {@code regexp} package and so of Prometheus; see {@link Re2Pattern}. */
    RE2;

    public String settingValue() {
        return name().toLowerCase(Locale.ROOT);
    }
}
