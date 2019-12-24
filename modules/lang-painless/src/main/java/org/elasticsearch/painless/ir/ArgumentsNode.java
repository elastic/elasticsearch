/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.painless.ir;

import org.elasticsearch.painless.Location;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class ArgumentsNode extends ExpressionNode {

    /* ---- begin tree structure ---- */

    protected List<ExpressionNode> argumentNodes = new ArrayList<>();

    public ArgumentsNode addArgumentNode(ExpressionNode argumentNode) {
        argumentNodes.add(argumentNode);
        return this;
    }

    public ArgumentsNode addArgumentNodes(Collection<ExpressionNode> argumentNodes) {
        this.argumentNodes.addAll(argumentNodes);
        return this;
    }

    public ArgumentsNode setArgumentNode(int index, ExpressionNode argumentNode) {
        argumentNodes.set(index, argumentNode);
        return this;
    }

    public ExpressionNode getArgumentNode(int index) {
        return argumentNodes.get(index);
    }

    public ArgumentsNode removeArgumentNode(ExpressionNode argumentNode) {
        argumentNodes.remove(argumentNode);
        return this;
    }

    public ArgumentsNode removeArgumentNode(int index) {
        argumentNodes.remove(index);
        return this;
    }

    public int getArgumentsSize() {
        return argumentNodes.size();
    }

    public List<ExpressionNode> getArgumentsNodes() {
        return argumentNodes;
    }

    public ArgumentsNode clearArgumentNodes() {
        argumentNodes.clear();
        return this;
    }

    @Override
    public ArgumentsNode setTypeNode(TypeNode typeNode) {
        super.setTypeNode(typeNode);
        return this;
    }

    /* ---- end tree structure, begin node data ---- */

    @Override
    public ArgumentsNode setLocation(Location location) {
        super.setLocation(location);
        return this;
    }

    /* ---- end node data ---- */

    public ArgumentsNode() {
        // do nothing
    }
}
