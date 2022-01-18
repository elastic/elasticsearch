/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.ir;

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.phase.IRTreeVisitor;

import java.util.ArrayList;
import java.util.List;

public class MapInitializationNode extends ExpressionNode {

    /* ---- begin tree structure ---- */

    private final List<ExpressionNode> keyNodes = new ArrayList<>();
    private final List<ExpressionNode> valueNodes = new ArrayList<>();

    public void addArgumentNode(ExpressionNode keyNode, ExpressionNode valueNode) {
        keyNodes.add(keyNode);
        valueNodes.add(valueNode);
    }

    public ExpressionNode getKeyNode(int index) {
        return keyNodes.get(index);
    }

    public ExpressionNode getValueNode(int index) {
        return valueNodes.get(index);
    }

    public int getArgumentsSize() {
        return keyNodes.size();
    }

    public List<ExpressionNode> getKeyNodes() {
        return keyNodes;
    }

    public List<ExpressionNode> getValueNodes() {
        return valueNodes;
    }

    /* ---- end tree structure, begin visitor ---- */

    @Override
    public <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        irTreeVisitor.visitMapInitialization(this, scope);
    }

    @Override
    public <Scope> void visitChildren(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        for (ExpressionNode keyNode : keyNodes) {
            keyNode.visit(irTreeVisitor, scope);
        }

        for (ExpressionNode valueNode : valueNodes) {
            valueNode.visit(irTreeVisitor, scope);
        }
    }

    /* ---- end visitor ---- */

    public MapInitializationNode(Location location) {
        super(location);
    }

}
