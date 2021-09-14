/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.ir;

import org.elasticsearch.painless.Location;

public abstract class BinaryNode extends ExpressionNode {

    /* ---- begin tree structure ---- */

    private ExpressionNode leftNode;
    private ExpressionNode rightNode;

    public void setLeftNode(ExpressionNode leftNode) {
        this.leftNode = leftNode;
    }

    public ExpressionNode getLeftNode() {
        return leftNode;
    }

    public void setRightNode(ExpressionNode rightNode) {
        this.rightNode = rightNode;
    }

    public ExpressionNode getRightNode() {
        return rightNode;
    }

    /* ---- end tree structure ---- */

    public BinaryNode(Location location) {
        super(location);
    }

}
