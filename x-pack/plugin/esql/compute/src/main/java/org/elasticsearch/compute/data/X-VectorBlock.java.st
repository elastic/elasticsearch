/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

$if(BytesRef)$
import org.apache.lucene.util.BytesRef;
$else$
$endif$
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Block view of a $Type$Vector.
 * This class is generated. Do not edit it.
 */
public final class $Type$VectorBlock extends AbstractVectorBlock implements $Type$Block {

    private final $Type$Vector vector;

    $Type$VectorBlock($Type$Vector vector) {
        super(vector.getPositionCount());
        this.vector = vector;
    }

    @Override
    public $Type$Vector asVector() {
        return vector;
    }

    @Override
$if(BytesRef)$
    public BytesRef getBytesRef(int valueIndex, BytesRef dest) {
        return vector.getBytesRef(valueIndex, dest);
$else$
    public $type$ get$Type$(int valueIndex) {
        return vector.get$Type$(valueIndex);
$endif$
    }

    @Override
    public int getTotalValueCount() {
        return vector.getPositionCount();
    }

    @Override
    public ElementType elementType() {
        return vector.elementType();
    }

    @Override
    public $Type$Block filter(int... positions) {
        return new Filter$Type$Vector(vector, positions).asBlock();
    }

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Block.class,
        "$Type$VectorBlock",
        $Type$VectorBlock::of
    );

    @Override
    public String getWriteableName() {
        return "$Type$VectorBlock";
    }

    static $Type$VectorBlock of(StreamInput in) throws IOException {
        final int positions = in.readVInt();
        final boolean constant = in.readBoolean();
        if (constant && positions > 0) {
            return new $Type$VectorBlock(new Constant$Type$Vector(in.read$Type$(), positions));
        } else {
            var builder = $Type$Vector.newVectorBuilder(positions);
            for (int i = 0; i < positions; i++) {
                builder.append$Type$(in.read$Type$());
            }
            return new $Type$VectorBlock(builder.build());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        final $Type$Vector vector = this.vector;
        final int positions = vector.getPositionCount();
        out.writeVInt(positions);
        out.writeBoolean(vector.isConstant());
        if (vector.isConstant() && positions > 0) {
$if(BytesRef)$
            out.write$Type$(get$Type$(0, new BytesRef()));
$else$
            out.write$Type$(get$Type$(0));
$endif$
        } else {
            for (int i = 0; i < positions; i++) {
$if(BytesRef)$
                out.write$Type$(get$Type$(i, new BytesRef()));
$else$
                out.write$Type$(get$Type$(i));
$endif$
            }
        }
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
        return getClass().getSimpleName() + "[vector=" + vector + "]";
    }
}
