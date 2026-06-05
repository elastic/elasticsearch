/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.tree;

import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.util.Collection;
import java.util.Iterator;

public abstract class NodeUtils {
    public static <A extends Node<A>, B extends Node<B>> String diffString(A left, B right) {
        return diffString(left.toString(), right.toString());
    }

    public static String diffString(String left, String right) {
        // break the strings into lines
        // then compare each line
        String[] leftSplit = left.split("\\n");
        String[] rightSplit = right.split("\\n");

        // find max - we could use streams but autoboxing is not cool
        int leftMaxPadding = 0;
        for (String string : leftSplit) {
            leftMaxPadding = Math.max(string.length(), leftMaxPadding);
        }

        // try to allocate the buffer - 5 represents the column comparison chars
        StringBuilder sb = new StringBuilder(left.length() + right.length() + Math.max(left.length(), right.length()) * 3);

        boolean leftAvailable = true, rightAvailable = true;
        for (int leftIndex = 0, rightIndex = 0; leftAvailable || rightAvailable; leftIndex++, rightIndex++) {
            String leftRow = "", rightRow = leftRow;
            if (leftIndex < leftSplit.length) {
                leftRow = leftSplit[leftIndex];
            } else {
                leftAvailable = false;
            }
            sb.append(leftRow);
            for (int i = leftRow.length(); i < leftMaxPadding; i++) {
                sb.append(" ");
            }
            // right side still available
            if (rightIndex < rightSplit.length) {
                rightRow = rightSplit[rightIndex];
            } else {
                rightAvailable = false;
            }
            if (leftAvailable || rightAvailable) {
                sb.append(leftRow.equals(rightRow) ? " = " : " ! ");
                sb.append(rightRow);
                sb.append("\n");
            }
        }
        return sb.toString();
    }

    private static final int TO_STRING_LIMIT = 52;

    public static void toString(
        StringBuilder sb,
        Collection<? extends Attribute> c,
        Node.NodeStringFormat format,
        NodeStringMapper mapper
    ) {
        // LIMITED truncates to keep human-readable toString bounded; FULL prints the whole list.
        // Both routes render each attribute through nodeString with the supplied format + mapper so
        // identifier mapping (anonymization) propagates correctly.
        if (format == Node.NodeStringFormat.LIMITED) {
            limitedToString(sb, c, format, mapper);
        } else {
            unlimitedToString(sb, c, format, mapper);
        }
    }

    private static void limitedToString(
        StringBuilder sb,
        Collection<? extends Attribute> c,
        Node.NodeStringFormat format,
        NodeStringMapper mapper
    ) {
        Iterator<? extends Attribute> it = c.iterator();
        if (it.hasNext() == false) {
            sb.append("[]");
            return;
        }

        // track how many characters we've added since the opening '[' for the truncation limit
        int start = sb.length();
        sb.append('[');
        for (;;) {
            Attribute a = it.next();
            StringBuilder render = new StringBuilder();
            if (a == null) {
                render.append("null");
            } else {
                a.nodeString(render, format, mapper);
            }
            String next = render.toString();
            int used = sb.length() - start;
            if (next.length() + used > TO_STRING_LIMIT) {
                sb.append(next, 0, Math.max(0, TO_STRING_LIMIT - used));
                sb.append('.').append('.').append(']');
                return;
            } else {
                sb.append(next);
            }
            if (it.hasNext() == false) {
                sb.append(']');
                return;
            }
            sb.append(',').append(' ');
        }
    }

    private static void unlimitedToString(
        StringBuilder sb,
        Collection<? extends Attribute> c,
        Node.NodeStringFormat format,
        NodeStringMapper mapper
    ) {
        sb.append('[');
        boolean first = true;
        for (Attribute s : c) {
            if (first == false) {
                sb.append(", ");
            }
            if (s == null) {
                sb.append("null");
            } else {
                s.nodeString(sb, format, mapper);
            }
            first = false;
        }
        sb.append(']');
    }
}
