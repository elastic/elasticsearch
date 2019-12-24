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

public abstract class ShiftNode extends BinaryNode {

    /* ---- begin tree structure ---- */

    protected TypeNode shiftTypeNode;

    public ShiftNode setShiftTypeNode(TypeNode shiftTypeNode) {
        this.shiftTypeNode = shiftTypeNode;
        return this;
    }

    public TypeNode getShiftTypeNode() {
        return shiftTypeNode;
    }

    public Class<?> getShiftType() {
        return shiftTypeNode.getType();
    }

    public String getShiftCanonicalTypeName() {
        return shiftTypeNode.getCanonicalTypeName();
    }

    @Override
    public ShiftNode setLeftNode(ExpressionNode leftNode) {
        super.setLeftNode(leftNode);
        return this;
    }

    @Override
    public ShiftNode setRightNode(ExpressionNode rightNode) {
        super.setRightNode(rightNode);
        return this;
    }

    @Override
    public ShiftNode setTypeNode(TypeNode typeNode) {
        super.setTypeNode(typeNode);
        return this;
    }

    /* ---- end tree structure, begin node data ---- */

    @Override
    public ShiftNode setLocation(Location location) {
        super.setLocation(location);
        return this;
    }

    /* ---- end node data ---- */

    public ShiftNode() {
        // do nothing
    }
}
