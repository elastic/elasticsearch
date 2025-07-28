/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.$Type$Block;

/**
 * Builds the resulting {@link $Type$Block} for some column in a top-n.
 * This class is generated. Edit {@code X-ResultBuilder.java.st} instead.
 */
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
    private $type$ key;

    ResultBuilderFor$Type$(BlockFactory blockFactory, TopNEncoder encoder, boolean inKey, int initialSize) {
$if(BytesRef)$
        this.encoder = encoder;
$else$
        assert encoder == TopNEncoder.DEFAULT_UNSORTABLE : encoder.toString();
$endif$
        this.inKey = inKey;
        this.builder = blockFactory.new$Type$BlockBuilder(initialSize);
    }

    @Override
    public void decodeKey(BytesRef keys) {
        assert inKey;
$if(BytesRef)$
        key = encoder.toSortable().decodeBytesRef(keys, scratch);
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
            case 1 -> builder.append$Type$(inKey ? key : readValueFromValues(values));
            default -> {
                builder.beginPositionEntry();
                for (int i = 0; i < count; i++) {
                    builder.append$Type$(readValueFromValues(values));
                }
                builder.endPositionEntry();
            }
        }
    }

    private $type$ readValueFromValues(BytesRef values) {
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
