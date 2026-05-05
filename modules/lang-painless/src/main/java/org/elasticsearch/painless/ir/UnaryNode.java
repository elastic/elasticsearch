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

public abstract class UnaryNode extends ExpressionNode {

    /* ---- begin tree structure ---- */

    private ExpressionNode childNode;

    public void setChildNode(ExpressionNode childNode) {
        this.childNode = childNode;
    }

    public ExpressionNode getChildNode() {
        return childNode;
    }

    /* ---- end tree structure, begin node data ---- */

    private Class<?> storeType;

    public void setStoreType(Class<?> storeType) {
        this.storeType = storeType;
    }

    public Class<?> getStoreType() {
        return storeType;
    }

    public String getStoreCanonicalTypeName() {
        return PainlessLookupUtility.typeToCanonicalTypeName(storeType);
    }

    /* ---- end node data ---- */

    public UnaryNode(Location location) {
        super(location);
    }

}
