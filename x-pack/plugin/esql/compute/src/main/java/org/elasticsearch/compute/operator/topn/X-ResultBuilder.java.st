/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
$if(Point)$
import org.elasticsearch.common.geo.SpatialPoint;
$endif$
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.$Type$Block;

class ResultBuilderFor$Type$ implements ResultBuilder {
    private final $Type$Block.Builder builder;

    private final boolean inKey;

$if(BytesRef)$
    private final TopNEncoder encoder;

    private final BytesRef scratch = new BytesRef();
$endif$

    /**
     * The value previously set by {@link #decodeKey}.
     */
$if(Point)$
    // TODO: we should be able to get rid of this object
    private SpatialPoint key;

$else$
    private $type$ key;
$endif$

    ResultBuilderFor$Type$(BlockFactory blockFactory, TopNEncoder encoder, boolean inKey, int initialSize) {
$if(BytesRef)$
        this.encoder = encoder;
$else$
        assert encoder == TopNEncoder.DEFAULT_UNSORTABLE : encoder.toString();
$endif$
        this.inKey = inKey;
        this.builder = $Type$Block.newBlockBuilder(initialSize, blockFactory);
    }

    @Override
    public void decodeKey(BytesRef keys) {
        assert inKey;
$if(BytesRef)$
        key = encoder.toSortable().decodeBytesRef(keys, scratch);
$elseif(Point)$
        key = TopNEncoder.DEFAULT_SORTABLE.decode$Type$(keys);
$else$
        key = TopNEncoder.DEFAULT_SORTABLE.decode$Type$(keys);
$endif$
    }

    @Override
    public void decodeValue(BytesRef values) {
        int count = TopNEncoder.DEFAULT_UNSORTABLE.decodeVInt(values);
        switch (count) {
            case 0 -> {
                builder.appendNull();
            }
$if(Point)$
            case 1 -> {
                SpatialPoint value = inKey ? key : readValueFromValues(values);
                builder.appendPoint(value.getX(), value.getY());
            }
$else$
            case 1 -> builder.append$Type$(inKey ? key : readValueFromValues(values));
$endif$
            default -> {
                builder.beginPositionEntry();
                for (int i = 0; i < count; i++) {
$if(Point)$
                    SpatialPoint point = readValueFromValues(values);
                    builder.appendPoint(point.getX(), point.getY());
$else$
                    builder.append$Type$(readValueFromValues(values));
$endif$
                }
                builder.endPositionEntry();
            }
        }
    }

$if(Point)$
    private SpatialPoint readValueFromValues(BytesRef values) {
$else$
    private $type$ readValueFromValues(BytesRef values) {
$endif$
$if(BytesRef)$
        return encoder.toUnsortable().decodeBytesRef(values, scratch);
$else$
        return TopNEncoder.DEFAULT_UNSORTABLE.decode$Type$(values);
$endif$
    }

    @Override
    public $Type$Block build() {
        return builder.build();
    }

    @Override
    public String toString() {
        return "ResultBuilderFor$Type$[inKey=" + inKey + "]";
    }

    @Override
    public void close() {
        builder.close();
    }
}
