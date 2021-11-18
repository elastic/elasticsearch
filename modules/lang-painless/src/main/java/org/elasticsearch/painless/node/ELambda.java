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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Lambda expression node.
 * <p>
 * This can currently only be the direct argument of a call (method/constructor).
 * When the argument is of a known type, it uses
 * <a href="http://cr.openjdk.java.net/~briangoetz/lambda/lambda-translation.html">
 * Java's lambda translation</a>. However, if its a def call, then we don't have
 * enough information, and have to defer this until link time. In that case a placeholder
 * and all captures are pushed onto the stack and folded into the signature of the parent call.
 * <p>
 * For example:
 * <br>
 * {@code def list = new ArrayList(); int capture = 0; list.sort((x,y) -> x - y + capture)}
 * <br>
 * is converted into a call (pseudocode) such as:
 * <br>
 * {@code sort(list, lambda$0, capture)}
 * <br>
 * At link time, when we know the interface type, this is decomposed with MethodHandle
 * combinators back into (pseudocode):
 * <br>
 * {@code sort(list, lambda$0(capture))}
 */
public class ELambda extends AExpression {

    private final List<String> canonicalTypeNameParameters;
    private final List<String> parameterNames;
    private final SBlock blockNode;

    public ELambda(
        int identifier,
        Location location,
        List<String> canonicalTypeNameParameters,
        List<String> parameterNames,
        SBlock blockNode
    ) {

        super(identifier, location);

        this.canonicalTypeNameParameters = Collections.unmodifiableList(Objects.requireNonNull(canonicalTypeNameParameters));
        this.parameterNames = Collections.unmodifiableList(Objects.requireNonNull(parameterNames));
        this.blockNode = Objects.requireNonNull(blockNode);
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

    @Override
    public <Scope> void visit(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        userTreeVisitor.visitLambda(this, scope);
    }

    @Override
    public <Scope> void visitChildren(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        blockNode.visit(userTreeVisitor, scope);
    }
}
