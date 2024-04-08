/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.node;

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.phase.UserTreeVisitor;

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

    public SFunction(
        int identifier,
        Location location,
        String returnCanonicalTypeName,
        String name,
        List<String> canonicalTypeNameParameters,
        List<String> parameterNames,
        SBlock blockNode,
        boolean isInternal,
        boolean isStatic,
        boolean isSynthetic,
        boolean isAutoReturnEnabled
    ) {

        super(identifier, location);

        this.returnCanonicalTypeName = Objects.requireNonNull(returnCanonicalTypeName);
        this.functionName = Objects.requireNonNull(name);
        this.canonicalTypeNameParameters = List.copyOf(canonicalTypeNameParameters);
        this.parameterNames = List.copyOf(parameterNames);
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
