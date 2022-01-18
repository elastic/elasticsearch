/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.ir;

import org.elasticsearch.painless.Location;

import java.util.ArrayList;
import java.util.List;

public abstract class ArgumentsNode extends ExpressionNode {

    /* ---- begin tree structure ---- */

    private final List<ExpressionNode> argumentNodes = new ArrayList<>();

    public void addArgumentNode(ExpressionNode argumentNode) {
        argumentNodes.add(argumentNode);
    }

    public List<ExpressionNode> getArgumentNodes() {
        return argumentNodes;
    }

    /* ---- end tree structure ---- */

    public ArgumentsNode(Location location) {
        super(location);
    }

}
