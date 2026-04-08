/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.common.bytes.PagedBytesCursor;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.$Type$Block;

/**
 * Builds the resulting {@link $Type$Block} for some column in a top-n.
 * This class is generated. Edit {@code X-ResultBuilder.java.st} instead.
 */
class ResultBuilderFor$Type$ implements ResultBuilder {
    private final $Type$Block.Builder builder;

    private final boolean inKey;

    private final TopNEncoder encoder;

$if(BytesRef)$
    private final PagedBytesCursor scratch = new PagedBytesCursor();
$endif$

    /**
     * The value previously set by {@link #decodeKey}.
     */
$if(BytesRef)$
    private PagedBytesCursor key;
$else$
    private $type$ key;
$endif$

    ResultBuilderFor$Type$(BlockFactory blockFactory, TopNEncoder encoder, boolean inKey, int initialSize) {
        this.encoder = encoder;
        this.inKey = inKey;
        this.builder = blockFactory.new$Type$BlockBuilder(initialSize);
    }

    @Override
    public void decodeKey(PagedBytesCursor keys, boolean asc) {
        assert inKey;
$if(BytesRef)$
        key = encoder.toSortable(asc).decodeBytesRef(keys, scratch);
$else$
        key = encoder.toSortable(asc).decode$Type$(keys);
$endif$
    }

    @Override
    public void decodeValue(PagedBytesCursor cursor) {
        int count = cursor.readVInt();
        switch (count) {
            case 0 -> {
                builder.appendNull();
            }
            case 1 -> builder.$if(BytesRef)$append$else$append$Type$$endif$(inKey ? key : readValueFromValues(cursor));
            default -> {
                builder.beginPositionEntry();
                for (int i = 0; i < count; i++) {
                    builder.$if(BytesRef)$append$else$append$Type$$endif$(readValueFromValues(cursor));
                }
                builder.endPositionEntry();
            }
        }
    }

$if(BytesRef)$
    private PagedBytesCursor readValueFromValues(PagedBytesCursor cursor) {
$else$
    private $type$ readValueFromValues(PagedBytesCursor cursor) {
$endif$
$if(BytesRef)$
        return encoder.toUnsortable().decodeBytesRef(cursor, scratch);
$else$
        return TopNEncoder.DEFAULT_UNSORTABLE.decode$Type$(cursor);
$endif$
    }

    @Override
    public void appendNull() {
        builder.appendNull();
    }

    @Override
    public void appendFromKey() {
        builder.$if(BytesRef)$append$else$append$Type$$endif$(key);
    }

    @Override
    public $Type$Block build() {
        return builder.build();
    }

    @Override
    public long estimatedBytes() {
        return builder.estimatedBytes();
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
