/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.ir;

import org.elasticsearch.painless.Location;

public abstract class ConditionNode extends StatementNode {

    /* ---- begin tree structure ---- */

    private ExpressionNode conditionNode;
    private BlockNode blockNode;

    public void setConditionNode(ExpressionNode conditionNode) {
        this.conditionNode = conditionNode;
    }

    public ExpressionNode getConditionNode() {
        return conditionNode;
    }

    public void setBlockNode(BlockNode blockNode) {
        this.blockNode = blockNode;
    }

    public BlockNode getBlockNode() {
        return blockNode;
    }

    /* ---- end tree structure ---- */

    public ConditionNode(Location location) {
        super(location);
    }

}
