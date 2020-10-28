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
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.phase.IRTreeVisitor;

public class ForEachSubArrayNode extends LoopNode {

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
