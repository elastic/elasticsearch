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
 * Represents a user-defined function.
 */
public class SFunction extends ANode {

    private final String returnCanonicalTypeName;
    private final String functionName;
    private final List<String> canonicalTypeNameParameters;
    private final List<String> parameterNames;
    private final SBlock blockNode;
    private final boolean isInternal;
    private final boolean isStatic;
    private final boolean isSynthetic;
    private final boolean isAutoReturnEnabled;

    public SFunction(int identifier, Location location,
            String returnCanonicalTypeName, String name, List<String> canonicalTypeNameParameters, List<String> parameterNames,
            SBlock blockNode,
            boolean isInternal, boolean isStatic, boolean isSynthetic, boolean isAutoReturnEnabled) {

        super(identifier, location);

        this.returnCanonicalTypeName = Objects.requireNonNull(returnCanonicalTypeName);
        this.functionName = Objects.requireNonNull(name);
        this.canonicalTypeNameParameters = Collections.unmodifiableList(Objects.requireNonNull(canonicalTypeNameParameters));
        this.parameterNames = Collections.unmodifiableList(Objects.requireNonNull(parameterNames));
        this.blockNode = Objects.requireNonNull(blockNode);
        this.isInternal = isInternal;
        this.isSynthetic = isSynthetic;
        this.isStatic = isStatic;
        this.isAutoReturnEnabled = isAutoReturnEnabled;
    }

    public String getReturnCanonicalTypeName() {
        return returnCanonicalTypeName;
    }

    public String getFunctionName() {
        return functionName;
    }

    public List<String> getCanonicalTypeNameParameters() {
        return canonicalTypeNameParameters;
    }

    public List<String> getParameterNames() {
        return parameterNames;
    }

    public SBlock getBlockNode() {
        return blockNode;
    }

    public boolean isInternal() {
        return isInternal;
    }

    public boolean isStatic() {
        return isStatic;
    }

    public boolean isSynthetic() {
        return isSynthetic;
    }

    /**
     * If set to {@code true} default return values are inserted if
     * not all paths return a value.
     */
    public boolean isAutoReturnEnabled() {
        return isAutoReturnEnabled;
    }

    @Override
    public <Scope> void visit(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        userTreeVisitor.visitFunction(this, scope);
    }

    @Override
    public <Scope> void visitChildren(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        blockNode.visit(userTreeVisitor, scope);
    }
}
