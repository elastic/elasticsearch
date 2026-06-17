/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.QueryPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.HashMap;
import java.util.Map;

public class BucketColumnMetadata {

    public static Map<NameId, Map<String, Object>> createColumnMetadata(LogicalPlan optimizedPlan, FoldContext foldContext) {
        Map<NameId, Map<String, Object>> resolved = new HashMap<>();
        // this does not retain bucket meta when executing row
        // we can fix this later by adding meta from analyzed plan as well
        optimizedPlan.forEachExpressionDown(Alias.class, alias -> {
            if (alias.child() instanceof Bucket bucket) {
                BucketIntervalMetadata intervalMetadata = bucket.getIntervalMetadata(foldContext);
                if (intervalMetadata != null) {
                    resolved.put(alias.id(), intervalMetadata.toColumnInfoMetaMap());
                }
            }
        });
        return resolved;
    }

    /**
     * Searches {@code plan} for an {@link Alias} whose id matches {@code attributeId}
     * and whose child is a {@link Bucket}. Returns that bucket's interval metadata,
     * or {@code null} if no such alias/bucket is found or the bucket's parameters
     * are not foldable.
     */
    @Nullable
    public static BucketIntervalMetadata findBucketMetadataForAttribute(QueryPlan<?> plan, NameId attributeId, FoldContext foldContext) {
        Holder<BucketIntervalMetadata> result = new Holder<>();
        plan.forEachExpressionDown(Alias.class, alias -> {
            if (result.get() == null && alias.id().equals(attributeId) && alias.child() instanceof Bucket bucket) {
                result.set(bucket.getIntervalMetadata(foldContext));
            }
        });
        return result.get();
    }
}
