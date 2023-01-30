/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

$if(BytesRef)$
import org.apache.lucene.util.BytesRef;
$endif$

/**
 * Vector that stores $type$ values.
 * This class is generated. Do not edit it.
 */
public sealed interface $Type$Vector extends Vector permits Constant$Type$Vector,Filter$Type$Vector,$Type$ArrayVector {

$if(BytesRef)$
    BytesRef getBytesRef(int position, BytesRef dest);

$else$
    $type$ get$Type$(int position);
$endif$

    @Override
    $Type$Block asBlock();

    @Override
    $Type$Vector filter(int... positions);

    static Builder newVectorBuilder(int estimatedSize) {
        return new $Type$VectorBuilder(estimatedSize);
    }

    sealed interface Builder extends Vector.Builder permits $Type$VectorBuilder {
        /**
         * Appends a $type$ to the current entry.
         */
        Builder append$Type$($type$ value);

        @Override
        $Type$Vector build();
    }
}
