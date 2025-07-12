/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.fuse;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;

import java.util.HashMap;
import java.util.Map;

public record FuseConfig(Fuse.FuseType fuseType, MapExpression options) {
    private static final double RANK_CONSTANT = 60;

    public double rankConstant() {
        if (options == null) {
            return RANK_CONSTANT;
        }

        Expression rankConstant = options.get("rank_constant");
        if (rankConstant == null) {
            return RANK_CONSTANT;
        }
        return (Integer) rankConstant.fold(FoldContext.small());
    }

    public String normalizer() {
        if (options == null) {
            return "none";
        }

        Expression normalizer = options.get("normalizer");
        if (normalizer == null) {
            return "none";
        }
        BytesRef normalizerBytes = (BytesRef) normalizer.fold(FoldContext.small());

        return normalizerBytes.utf8ToString();
    }

    public Map<String, Double> weights() {
        Map<String, Double> result = new HashMap<>();
        if (options == null) {
            return result;
        }

        MapExpression weights = (MapExpression) options.get("weights");
        if (weights == null) {
            return result;
        }

        weights.keyFoldedMap().forEach((k, v) -> { result.put(k, (double) v.fold(FoldContext.small())); });
        return result;
    }
}
