/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.function.Predicate;

class LucenePushDownUtils {
    public static boolean hasIdenticalDelegate(FieldAttribute attr, SearchStats stats) {
        return stats.hasIdenticalDelegate(attr.name());
    }

    /**
     * We see fields as pushable if either they are aggregatable or they are indexed.
     * This covers non-indexed cases like <code>AbstractScriptFieldType</code> which hard-coded <code>isAggregatable</code> to true,
     * as well as normal <code>FieldAttribute</code>'s which can only be pushed down if they are indexed.
     * The reason we don't just rely entirely on <code>isAggregatable</code> is because this is often false for normal fields, and could
     * also differ from node to node, and we can physically plan each node separately, allowing Lucene pushdown on the nodes that
     * support it, and relying on the compute engine for the nodes that do not.
     */
    public static boolean isPushableFieldAttribute(
        Expression exp,
        Predicate<FieldAttribute> hasIdenticalDelegate,
        Predicate<FieldAttribute> isIndexed
    ) {
        if (exp instanceof FieldAttribute fa && fa.getExactInfo().hasExact() && (fa.field().isAggregatable() || isIndexed.test(fa))) {
            return (fa.dataType() != DataType.TEXT && fa.dataType() != DataType.SEMANTIC_TEXT) || hasIdenticalDelegate.test(fa);
        }
        return false;
    }
}
