/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.tree;

import org.elasticsearch.xpack.esql.core.expression.NameId;

import java.util.List;

/**
 * Renders the properties of a {@link Node} as a string.
 */
class NodePropertiesToString {
    private final Node.NodeStringFormat format;
    private final Node<?> node;
    private final boolean skipIfChild;
    private final StringBuilder sb;
    private int charactersRemainingInLine;
    private int linesUsed = 0;

    NodePropertiesToString(StringBuilder sb, Node.NodeStringFormat format, Node<?> node, boolean skipIfChild) {
        this.sb = sb;
        this.format = format;
        this.node = node;
        this.skipIfChild = skipIfChild;
        this.charactersRemainingInLine = format.maxWidth;
    }

    /**
     * Render the properties of this {@link Node} one by
     * one like {@code foo bar baz}. These go inside the
     * {@code [} and {@code ]} of the output of {@link NodeToString#treeString}.
     */
    void propertiesToString() {
        List<Object> props = node.nodeProperties();
        int remainingProperties = format.maxProperties;
        boolean firstProperty = true;
        for (Object prop : props) {
            // if skipping children, check skip if this is a child
            if (skipIfChild && (node.children().contains(prop) || node.children().equals(prop))) {
                continue;
            }
            if (remainingProperties-- < 0) {
                sb.append("...").append(props.size() - format.maxProperties).append("fields not shown");
                break;
            }

            if (firstProperty) {
                firstProperty = false;
            } else {
                appendString(",");
            }
            boolean canContinue = prop instanceof Iterable<?> iterable ? appendIterable(iterable) : appendString(propertyToString(prop));
            if (canContinue == false) {
                break;
            }
        }
    }

    /**
     * Append {@code stringValue} to {@link #sb}, wrapping at line boundaries.
     * Returns {@code true} if rendering can continue, {@code false} if the line budget is exhausted.
     */
    private boolean appendString(String stringValue) {
        int start = 0;
        while (stringValue.length() - start > charactersRemainingInLine) {
            sb.append(stringValue, start, start + charactersRemainingInLine);
            if (linesUsed >= format.maxLines - 1) {
                sb.append("...");
                return false;
            }
            sb.append("\n");
            linesUsed++;
            start += charactersRemainingInLine;
            charactersRemainingInLine = format.maxWidth;
        }
        sb.append(stringValue, start, stringValue.length());
        charactersRemainingInLine -= stringValue.length() - start;
        return true;
    }

    private boolean appendIterable(Iterable<?> iterable) {
        if (appendString("[") == false) {
            return false;
        }
        boolean firstElement = true;
        for (Object element : iterable) {
            if (firstElement == false) {
                if (appendString(", ") == false) {
                    return false;
                }
            }
            if (appendString(propertyToString(element)) == false) {
                return false;
            }
            firstElement = false;
        }
        return appendString("]");
    }

    private String propertyToString(Object obj) {
        return switch (obj) {
            case null -> "null";
            case Node<?> n -> {
                /*
                 * We still build a string here which we then cut up. But this is only
                 * for things like the expression tree. Most other nodes are skipped
                 * and rendered as proper children, properly sharing the StringBuilder.
                 */
                StringBuilder str = new StringBuilder();
                n.nodeString(str, format);
                yield str.toString();
            }
            case NameId nameId -> "#" + obj;
            default -> String.valueOf(obj);
        };
    }
}
