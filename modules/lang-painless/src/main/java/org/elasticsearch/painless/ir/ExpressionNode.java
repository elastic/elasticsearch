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

import java.util.List;

public abstract class ExpressionNode extends IRNode {

    private Class<?> expressionType;
    private List<String> captureNames = List.of();
    private boolean instanceCapture;
    private boolean captureBox;

    public void setExpressionType(Class<?> expressionType) {
        this.expressionType = expressionType;
    }

    public Class<?> getExpressionType() {
        return expressionType;
    }

    public String getExpressionCanonicalTypeName() {
        return PainlessLookupUtility.typeToCanonicalTypeName(expressionType);
    }

    public void setCaptureNames(List<String> captureNames) {
        this.captureNames = List.copyOf(captureNames);
    }

    public List<String> getCaptureNames() {
        return captureNames;
    }

    public void setInstanceCapture(boolean instanceCapture) {
        this.instanceCapture = instanceCapture;
    }

    public boolean hasInstanceCapture() {
        return instanceCapture;
    }

    public void setCaptureBox(boolean captureBox) {
        this.captureBox = captureBox;
    }

    public boolean hasCaptureBox() {
        return captureBox;
    }

    public ExpressionNode(Location location) {
        super(location);
    }

}
