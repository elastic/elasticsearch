/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

/**
 * How large a vocabulary a column may leave behind for the merge that reads it next.
 *
 * <p>This is not {@link DictionaryPolicy}, though both bound term bytes, because the two answer to
 * different readers. A dictionary serves this column, so it is also held to a share of it: one as large as
 * the values it stands in for has bought nothing. A vocabulary serves the merge, which reads it against a
 * column many times larger, so a share of this column says nothing useful about it and only the absolute
 * bound applies.
 *
 * @param maxBytes the most term bytes a column may leave behind, or zero to leave none
 */
public record SummaryPolicy(int maxBytes) {

    /** Leaves nothing behind, so a merge reads the values instead. */
    public static final SummaryPolicy NONE = new SummaryPolicy(0);

    public SummaryPolicy {
        if (maxBytes < 0) {
            throw new IllegalArgumentException("maxBytes must not be negative, got " + maxBytes);
        }
    }

    public boolean enabled() {
        return maxBytes > 0;
    }
}
