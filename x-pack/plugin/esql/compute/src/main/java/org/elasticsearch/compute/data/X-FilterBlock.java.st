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
 * Filter block for $Type$Blocks.
 * This class is generated. Do not edit it.
 */
final class Filter$Type$Block extends AbstractFilterBlock implements $Type$Block {

    private final $Type$Block block;

    Filter$Type$Block($Type$Block block, int... positions) {
        super(block, positions);
        this.block = block;
    }

    @Override
    public $Type$Vector asVector() {
        return null;
    }

    @Override
$if(BytesRef)$
    public BytesRef getBytesRef(int valueIndex, BytesRef dest) {
        return block.getBytesRef(valueIndex, dest);
$else$
    public $type$ get$Type$(int valueIndex) {
        return block.get$Type$(valueIndex);
$endif$
    }

    @Override
    public ElementType elementType() {
        return ElementType.$TYPE$;
    }

    @Override
    public $Type$Block filter(int... positions) {
        return new Filter$Type$Block(this, positions);
    }

    @Override
    public $Type$Block expand() {
        if (false == block.mayHaveMultivaluedFields()) {
            return this;
        }
        /*
         * Build a copy of the target block, selecting only the positions
         * we've been assigned and expanding all multivalued fields
         * into single valued fields.
         */
        $Type$Block.Builder builder = $Type$Block.newBlockBuilder(positions.length);
$if(BytesRef)$
        BytesRef scratch = new BytesRef();
$endif$
        for (int p : positions) {
            if (block.isNull(p)) {
                builder.appendNull();
                continue;
            }
            int start = block.getFirstValueIndex(p);
            int end = start + block.getValueCount(p);
            for (int i = start; i < end; i++) {
$if(BytesRef)$
                BytesRef v = block.getBytesRef(i, scratch);
                builder.appendBytesRef(v);
$else$
                builder.append$Type$(block.get$Type$(i));
$endif$
            }
        }
        return builder.build();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof $Type$Block that) {
            return $Type$Block.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return $Type$Block.hash(this);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName());
        sb.append("[positions=" + getPositionCount() + ", values=[");
        appendValues(sb);
        sb.append("]]");
        return sb.toString();
    }

    private void appendValues(StringBuilder sb) {
        final int positions = getPositionCount();
        for (int p = 0; p < positions; p++) {
            if (p > 0) {
                sb.append(", ");
            }
            int start = getFirstValueIndex(p);
            int count = getValueCount(p);
            if (count == 1) {
$if(BytesRef)$
                sb.append(get$Type$(start, new BytesRef()));
$else$
                sb.append(get$Type$(start));
$endif$
                continue;
            }
            sb.append('[');
            int end = start + count;
            for (int i = start; i < end; i++) {
                if (i > start) {
                    sb.append(", ");
                }
$if(BytesRef)$
                sb.append(get$Type$(i, new BytesRef()));
$else$
                sb.append(get$Type$(i));
$endif$
            }
            sb.append(']');
        }
    }
}
