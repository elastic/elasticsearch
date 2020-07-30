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

package org.elasticsearch.painless.node;

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.phase.UserTreeVisitor;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents a map initialization shortcut.
 */
public class EMapInit extends AExpression {

    private final List<AExpression> keyNodes;
    private final List<AExpression> valueNodes;

    public EMapInit(int identifier, Location location, List<AExpression> keyNodes, List<AExpression> valueNodes) {
        super(identifier, location);

        this.keyNodes = Collections.unmodifiableList(Objects.requireNonNull(keyNodes));
        this.valueNodes = Collections.unmodifiableList(Objects.requireNonNull(valueNodes));
    }

    public List<AExpression> getKeyNodes() {
        return keyNodes;
    }

    public List<AExpression> getValueNodes() {
        return valueNodes;
    }

    @Override
    public <Scope> void visit(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        userTreeVisitor.visitMapInit(this, scope);
    }

    @Override
    public <Scope> void visitChildren(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        for (AExpression keyNode : keyNodes) {
            keyNode.visit(userTreeVisitor, scope);
        }

        for (AExpression valueNode : valueNodes) {
            valueNode.visit(userTreeVisitor, scope);
        }
    }
}
