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

import java.util.HashMap;
import java.util.Map;

public class BucketColumnMetadata {

    public static Map<NameId, Map<String, Object>> createColumnMetadata(LogicalPlan optimizedPlan, FoldContext foldContext) {
        var resolved = new HashMap<NameId, Map<String, Object>>();
        // this does not retain bucket meta when executing row
        // we can fix this later by adding meta from analyzed plan as well
        optimizedPlan.forEachExpressionDown(Alias.class, alias -> {
            if (alias.child() instanceof Bucket bucket) {
                Map<String, Object> intervalMetadata = bucket.getIntervalMetadata(foldContext);
                if (intervalMetadata != null) {
                    resolved.put(alias.id(), intervalMetadata);
                }
            }
        });
        return resolved;
    }
}
