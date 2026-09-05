/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action.suggestions;

/**
 * Test-only helper that lets a test write the cursor position inline in the query text via a
 * {@code <*>} marker, instead of a bare integer offset next to a separately-written query string
 * (or {@code indexOf(...) + N} arithmetic with a comment explaining where it lands). Deriving the
 * cursor from the marker keeps the query and its cursor position visually attached and self-
 * evident, and fails loudly if a test accidentally omits the marker or includes more than one.
 */
public record CursorMarker(String query, int cursor) {

    public static final String MARKER = "<*>";

    public static CursorMarker of(String textWithMarker) {
        int index = textWithMarker.indexOf(MARKER);
        if (index < 0) {
            throw new IllegalArgumentException("missing " + MARKER + " marker in: " + textWithMarker);
        }
        if (textWithMarker.indexOf(MARKER, index + 1) >= 0) {
            throw new IllegalArgumentException("more than one " + MARKER + " marker in: " + textWithMarker);
        }
        return new CursorMarker(textWithMarker.substring(0, index) + textWithMarker.substring(index + MARKER.length()), index);
    }
}
