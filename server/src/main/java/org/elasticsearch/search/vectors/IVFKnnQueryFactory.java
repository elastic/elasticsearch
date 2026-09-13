/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.vectors;

import org.apache.lucene.search.Query;

/**
 * Rebuilds an {@link AbstractIVFKnnVectorQuery} as the same concrete type, copying everything except
 * {@code filter}, {@code k}, {@code numCands} and {@code postFilterDelegate}. Centralizing this here
 * means a new IVF query type cannot silently inherit another type's reconstruction: add a switch arm
 * or respawn fails loudly.
 */
final class IVFKnnQueryFactory {

    private IVFKnnQueryFactory() {}

    static AbstractIVFKnnVectorQuery cloneWithParams(
        AbstractIVFKnnVectorQuery query,
        Query filter,
        int k,
        int numCands,
        boolean postFilterDelegate
    ) {
        return switch (query) {
            case DiversifyingChildrenIVFKnnFloatSlicedVectorQuery q -> new DiversifyingChildrenIVFKnnFloatSlicedVectorQuery(
                q.field,
                q.query,
                k,
                numCands,
                filter,
                q.parentsFilter,
                q.providedVisitRatio,
                q.ivfQueryConfigResolver,
                postFilterDelegate,
                q.sliceField,
                q.sliceIds
            );
            case DiversifyingChildrenIVFKnnFloatVectorQuery q -> new DiversifyingChildrenIVFKnnFloatVectorQuery(
                q.field,
                q.query,
                k,
                numCands,
                filter,
                q.parentsFilter,
                q.providedVisitRatio,
                q.ivfQueryConfigResolver,
                postFilterDelegate
            );
            case IVFKnnFloatSlicedVectorQuery q -> new IVFKnnFloatSlicedVectorQuery(
                q.field,
                q.query,
                k,
                numCands,
                filter,
                q.providedVisitRatio,
                q.ivfQueryConfigResolver,
                postFilterDelegate,
                q.sliceField,
                q.sliceIds
            );
            case IVFKnnFloatVectorQuery q -> new IVFKnnFloatVectorQuery(
                q.field,
                q.query,
                k,
                numCands,
                filter,
                q.providedVisitRatio,
                q.ivfQueryConfigResolver,
                postFilterDelegate
            );
            case DiversifyingChildrenIVFKnnByteSlicedVectorQuery q -> new DiversifyingChildrenIVFKnnByteSlicedVectorQuery(
                q.field,
                q.query,
                k,
                numCands,
                filter,
                q.parentsFilter,
                q.providedVisitRatio,
                q.ivfQueryConfigResolver,
                postFilterDelegate,
                q.sliceField,
                q.sliceIds
            );
            case DiversifyingChildrenIVFKnnByteVectorQuery q -> new DiversifyingChildrenIVFKnnByteVectorQuery(
                q.field,
                q.query,
                k,
                numCands,
                filter,
                q.parentsFilter,
                q.providedVisitRatio,
                q.ivfQueryConfigResolver,
                postFilterDelegate
            );
            case IVFKnnByteSlicedVectorQuery q -> new IVFKnnByteSlicedVectorQuery(
                q.field,
                q.query,
                k,
                numCands,
                filter,
                q.providedVisitRatio,
                q.ivfQueryConfigResolver,
                postFilterDelegate,
                q.sliceField,
                q.sliceIds
            );
            case IVFKnnByteVectorQuery q -> new IVFKnnByteVectorQuery(
                q.field,
                q.query,
                k,
                numCands,
                filter,
                q.providedVisitRatio,
                q.ivfQueryConfigResolver,
                postFilterDelegate
            );
            default -> throw new IllegalStateException("unknown IVF query type [" + query.getClass().getName() + "]");
        };
    }
}
