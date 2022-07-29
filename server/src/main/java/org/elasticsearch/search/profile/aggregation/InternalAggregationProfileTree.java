/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile.aggregation;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.profile.AbstractInternalProfileTree;

public class InternalAggregationProfileTree extends AbstractInternalProfileTree<AggregationProfileBreakdown, Aggregator> {

    @Override
    protected AggregationProfileBreakdown createProfileBreakdown() {
        return new AggregationProfileBreakdown();
    }

    @Override
    protected String getTypeFromElement(Aggregator element) {
        return typeFromAggregator(element);
    }

    @Override
    protected String getDescriptionFromElement(Aggregator element) {
        return element.name();
    }

    public static String typeFromAggregator(Aggregator aggregator) {
        // Anonymous classes (such as NonCollectingAggregator in TermsAgg) won't have a name,
        // we need to get the super class
        if (aggregator.getClass().getSimpleName().isEmpty()) {
            return aggregator.getClass().getSuperclass().getSimpleName();
        }
        Class<?> enclosing = aggregator.getClass().getEnclosingClass();
        if (enclosing != null) {
            return enclosing.getSimpleName() + "." + aggregator.getClass().getSimpleName();
        }
        return aggregator.getClass().getSimpleName();
    }
}
