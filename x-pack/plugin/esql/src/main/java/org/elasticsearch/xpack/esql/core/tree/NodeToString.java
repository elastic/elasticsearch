/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.tree;

import java.util.BitSet;
import java.util.List;

/**
 * Renders {@link Node} trees as strings.
 */
class NodeToString {
    private final StringBuilder sb = new StringBuilder();
    private final BitSet hasParentPerDepth = new BitSet();
    private final Node.NodeStringFormat format;

    NodeToString(Node.NodeStringFormat format) {
        this.format = format;
    }

    /**
     * Render this {@link Node} as a tree like
     * {@snippet lang=txt :
     * Project[[i{f}#0]]
     * \_Filter[i{f}#1]
     *   \_SubQueryAlias[test]
     *     \_EsRelation[test][i{f}#2]
     * }
     */
    StringBuilder treeString(Node<?> node, int depth) {
        indent(depth);

        node.nodeString(sb, format);

        List<? extends Node<?>> children = node.children();
        if (children.isEmpty() == false) {
            sb.append("\n");
        }
        for (int i = 0; i < children.size(); i++) {
            Node<?> t = children.get(i);
            hasParentPerDepth.set(depth, i < children.size() - 1);
            treeString(t, depth + 1);
            if (i < children.size() - 1) {
                sb.append("\n");
            }
        }
        return sb;
    }

    private void indent(int depth) {
        if (depth == 0) {
            return;
        }
        // draw children
        for (int column = 0; column < depth; column++) {
            if (hasParentPerDepth.get(column)) {
                sb.append("|");
                // if not the last elder, adding padding (since each column has two chars ("|_" or "\_")
                if (column < depth - 1) {
                    sb.append(" ");
                }
            } else {
                // if the child has no parent (elder on the previous level), it means its the last sibling
                sb.append((column == depth - 1) ? "\\" : "  ");
            }
        }
        sb.append("_");
    }

}
