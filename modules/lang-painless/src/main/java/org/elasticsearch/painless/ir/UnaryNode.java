/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.ir;

import org.elasticsearch.painless.Location;

public abstract class UnaryNode extends ExpressionNode {

    /* ---- begin tree structure ---- */

    private ExpressionNode childNode;

    public void setChildNode(ExpressionNode childNode) {
        this.childNode = childNode;
    }

    public ExpressionNode getChildNode() {
        return childNode;
    }

    /* ---- end tree structure ---- */

    public UnaryNode(Location location) {
        super(location);
    }

}
