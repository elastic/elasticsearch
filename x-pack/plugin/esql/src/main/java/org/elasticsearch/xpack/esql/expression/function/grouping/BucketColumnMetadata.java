/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class BucketColumnMetadata {

    public interface Resolver {
        void add(NameId id, Map<String, Object> metadata);

        Collection<Map<String, Object>> all();
    }

    public static Resolver createResolver(LogicalPlan plan, FoldContext foldContext) {
        var resolved = new HashMap<NameId, Map<String, Object>>();
        plan.forEachExpressionDown(Alias.class, alias -> {
            if (alias.child() instanceof Bucket bucket) {
                Map<String, Object> intervalMetadata = bucket.getIntervalMetadata(foldContext);
                if (intervalMetadata != null) {
                    resolved.put(alias.id(), intervalMetadata);
                }
            }
        });
        return new Resolver() {
            @Override
            public void add(NameId id, Map<String, Object> metadata) {
                Map<String, Object> intervalMetadata = resolved.get(id);
                if (intervalMetadata != null) {
                    metadata.putAll(intervalMetadata);
                }
            }

            @Override
            public Collection<Map<String, Object>> all() {
                return resolved.values();
            }
        };
    }
}
