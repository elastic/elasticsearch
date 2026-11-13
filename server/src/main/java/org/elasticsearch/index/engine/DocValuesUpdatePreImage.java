/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

/**
 * Names the companion doc-values columns that hold a field's value as it was before its first in-place update.
 * <p>
 * With synthetic recovery source a past operation's {@code _source} is rebuilt from the current doc values, which an in-place update has
 * overwritten; the pre-image lets the original index operation reconstruct with its own value. The value column mirrors the field's own
 * doc-values type so it can be overlaid in place of the live column, and the marker column records that a pre-image was captured even
 * when the captured value was empty. Both live on the base document, so {@link PruningMergePolicy} drops them on the same retention
 * boundary as {@code _recovery_source}.
 */
public final class DocValuesUpdatePreImage {

    private static final String VALUE_PREFIX = "_dvp_v_";
    private static final String PRESENCE_PREFIX = "_dvp_p_";

    private DocValuesUpdatePreImage() {}

    /** The column holding the field's pre-image value(s), mirroring the field's own doc-values type. */
    public static String valueField(String field) {
        return VALUE_PREFIX + field;
    }

    /** The marker column, present once the field's pre-image has been captured, even when the captured value was empty. */
    public static String presenceField(String field) {
        return PRESENCE_PREFIX + field;
    }

    /** Whether the column is a pre-image companion, so merge can prune it alongside the recovery source. */
    public static boolean isPreImageField(String fieldName) {
        return fieldName.startsWith(VALUE_PREFIX) || fieldName.startsWith(PRESENCE_PREFIX);
    }

    /** The field a marker column belongs to, or {@code null} if it is not a marker column. */
    public static String fieldForPresenceColumn(String columnName) {
        return columnName.startsWith(PRESENCE_PREFIX) ? columnName.substring(PRESENCE_PREFIX.length()) : null;
    }
}
