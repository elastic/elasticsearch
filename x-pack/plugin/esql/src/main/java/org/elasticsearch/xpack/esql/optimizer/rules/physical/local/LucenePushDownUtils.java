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
    /**
     * this method is supposed to be used to define if a field can be used for exact push down (eg. sort or filter).
     * "aggregatable" is the most accurate information we can have from field_caps as of now.
     * Pushing down operations on fields that are not aggregatable would result in an error.
     */
    public static boolean isAggregatable(FieldAttribute f) {
        return f.exactAttribute().field().isAggregatable();
    }

    public static boolean hasIdenticalDelegate(FieldAttribute attr, SearchStats stats) {
        return stats.hasIdenticalDelegate(attr.name());
    }

    public static boolean isPushableFieldAttribute(Expression exp, Predicate<FieldAttribute> hasIdenticalDelegate) {
        if (exp instanceof FieldAttribute fa && fa.getExactInfo().hasExact() && isAggregatable(fa)) {
            return fa.dataType() != DataType.TEXT || hasIdenticalDelegate.test(fa);
        }
        return false;
    }
}
