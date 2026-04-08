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
    private final StringBuilder result = new StringBuilder();
    private int charactersRemainingInLine;
    private int linesUsed = 0;

    NodePropertiesToString(Node.NodeStringFormat format, Node<?> node, boolean skipIfChild) {
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
    String propertiesToString() {
        List<Object> props = node.nodeProperties();
        int remainingProperties = format.maxProperties;
        boolean firstProperty = true;
        for (Object prop : props) {
            // if skipping children, check skip if this is a child
            if (skipIfChild && (node.children().contains(prop) || node.children().equals(prop))) {
                continue;
            }
            if (remainingProperties-- < 0) {
                result.append("...").append(props.size() - format.maxProperties).append("fields not shown");
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

        return result.toString();
    }

    /**
     * Append {@code stringValue} to {@link #result}, wrapping at line boundaries.
     * Returns {@code true} if rendering can continue, {@code false} if the line budget is exhausted.
     */
    private boolean appendString(String stringValue) {
        int start = 0;
        while (stringValue.length() - start > charactersRemainingInLine) {
            result.append(stringValue, start, start + charactersRemainingInLine);
            if (linesUsed >= format.maxLines - 1) {
                result.append("...");
                return false;
            }
            result.append("\n");
            linesUsed++;
            start += charactersRemainingInLine;
            charactersRemainingInLine = format.maxWidth;
        }
        result.append(stringValue, start, stringValue.length());
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
        if (obj == null) {
            return "null";
        }
        if (obj instanceof Node<?> n) {
            return n.nodeString(format);
        }
        if (obj instanceof NameId) {
            return "#" + obj;
        }
        return String.valueOf(obj);
    }
}
