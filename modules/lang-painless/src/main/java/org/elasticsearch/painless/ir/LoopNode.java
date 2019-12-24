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

import org.elasticsearch.painless.Locals.Variable;
import org.elasticsearch.painless.Location;

public abstract class LoopNode extends ConditionNode {

    /* ---- begin tree structure ---- */

    @Override
    public LoopNode setConditionNode(ExpressionNode conditionNode) {
        super.setConditionNode(conditionNode);
        return this;
    }

    @Override
    public LoopNode setBlockNode(BlockNode blockNode) {
        this.blockNode = blockNode;
        return this;
    }

    /* ---- end tree structure, begin node data ---- */

    protected boolean isContinuous;
    protected Variable loopCounter;

    public LoopNode setContinuous(boolean isContinuous) {
        this.isContinuous = isContinuous;
        return this;
    }

    public boolean isContinuous() {
        return isContinuous;
    }

    public LoopNode setLoopCounter(Variable loopCounter) {
        this.loopCounter = loopCounter;
        return this;
    }

    public Variable getLoopCounter() {
        return loopCounter;
    }

    @Override
    public LoopNode setLocation(Location location) {
        super.setLocation(location);
        return this;
    }

    /* ---- end node data ---- */

    public LoopNode() {
        // do nothing
    }
}
