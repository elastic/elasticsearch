/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.core.Nullable;

/**
 * Optional rewrite hint for the null misuse warning, emitted when an expression will always
 * evaluate to {@code NULL} because a child is an explicitly written {@code NULL}.
 * <p>
 * Most viral-null expressions have no better spelling and return {@code null}. Comparisons
 * that users often confuse with {@code IS NULL} implement this.
 */
public interface NullMisuseSuggestion {
    /**
     * Alternative the user may have meant, or {@code null} if there is no rewrite to suggest.
     */
    @Nullable
    String nullMisuseAlternative();
}
