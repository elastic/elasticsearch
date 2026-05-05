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
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.phase.IRTreeVisitor;

public class FieldNode extends IRNode {

    /* ---- begin node data ---- */

    private Class<?> fieldType;
    private int modifiers;
    private String name;

    public void setFieldType(Class<?> fieldType) {
        this.fieldType = fieldType;
    }

    public Class<?> getFieldType() {
        return fieldType;
    }

    public String getFieldCanonicalTypeName() {
        return PainlessLookupUtility.typeToCanonicalTypeName(fieldType);
    }

    public void setModifiers(int modifiers) {
        this.modifiers = modifiers;
    }

    public int getModifiers() {
        return modifiers;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    /* ---- end node data, begin visitor ---- */

    @Override
    public <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        irTreeVisitor.visitField(this, scope);
    }

    @Override
    public <Scope> void visitChildren(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        // do nothing; terminal node
    }

    /* ---- end visitor ---- */

    public FieldNode(Location location) {
        super(location);
    }

}
