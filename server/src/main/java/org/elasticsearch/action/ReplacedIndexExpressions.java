/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ReplacedIndexExpressions {
    protected final Map<String, ReplacedIndexExpression> replacedExpressionMap;

    public ReplacedIndexExpressions(Map<String, ReplacedIndexExpression> replacedExpressionMap) {
        this.replacedExpressionMap = replacedExpressionMap;
    }

    public String[] indices() {
        return ReplacedIndexExpression.toIndices(replacedExpressionMap);
    }

    public List<String> indicesAsList() {
        return List.of(indices());
    }

    public Map<String, ReplacedIndexExpression> asMap() {
        return replacedExpressionMap;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (ReplacedIndexExpressions) obj;
        return Objects.equals(this.replacedExpressionMap, that.replacedExpressionMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(replacedExpressionMap);
    }

    @Override
    public String toString() {
        return "ReplacedIndexExpressions[" + "replacedExpressionMap=" + replacedExpressionMap + ']';
    }
}
