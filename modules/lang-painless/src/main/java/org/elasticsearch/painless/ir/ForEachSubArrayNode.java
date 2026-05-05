/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless.ir;

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.phase.IRTreeVisitor;

public class ForEachSubArrayNode extends ConditionNode {

    /* ---- begin node data ---- */

    private Class<?> variableType;
    private String variableName;
    private PainlessCast cast;
    private Class<?> arrayType;
    private String arrayName;
    private Class<?> indexType;
    private String indexName;
    private Class<?> indexedType;

    public void setVariableType(Class<?> variableType) {
        this.variableType = variableType;
    }

    public Class<?> getVariableType() {
        return variableType;
    }

    public String getVariableCanonicalTypeName() {
        return PainlessLookupUtility.typeToCanonicalTypeName(variableType);
    }

    public void setVariableName(String variableName) {
        this.variableName = variableName;
    }

    public String getVariableName() {
        return variableName;
    }

    public void setCast(PainlessCast cast) {
        this.cast = cast;
    }

    public PainlessCast getCast() {
        return cast;
    }

    public void setArrayType(Class<?> arrayType) {
        this.arrayType = arrayType;
    }

    public Class<?> getArrayType() {
        return arrayType;
    }

    public String getArrayCanonicalTypeName() {
        return PainlessLookupUtility.typeToCanonicalTypeName(arrayType);
    }

    public void setArrayName(String arrayName) {
        this.arrayName = arrayName;
    }

    public String getArrayName() {
        return arrayName;
    }

    public void setIndexType(Class<?> indexType) {
        this.indexType = indexType;
    }

    public Class<?> getIndexType() {
        return indexType;
    }

    public String getIndexCanonicalTypeName() {
        return PainlessLookupUtility.typeToCanonicalTypeName(indexType);
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexedType(Class<?> indexedType) {
        this.indexedType = indexedType;
    }

    public Class<?> getIndexedType() {
        return indexedType;
    }

    public String getIndexedCanonicalTypeName() {
        return PainlessLookupUtility.typeToCanonicalTypeName(indexedType);
    }

    /* ---- end node data, begin visitor ---- */

    @Override
    public <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        irTreeVisitor.visitForEachSubArrayLoop(this, scope);
    }

    @Override
    public <Scope> void visitChildren(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        getConditionNode().visit(irTreeVisitor, scope);
        getBlockNode().visit(irTreeVisitor, scope);
    }

    /* ---- end visitor ---- */

    public ForEachSubArrayNode(Location location) {
        super(location);
    }

}
