/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.util.$if(boolean)$Bit$else$$Type$$endif$Array;
import org.elasticsearch.core.Releasable;

/**
 * Vector implementation that defers to an enclosed $Type$Array.
 * This class is generated. Do not edit it.
 */
public final class $Type$BigArrayVector extends AbstractVector implements $Type$Vector, Releasable {

    private final $if(boolean)$Bit$else$$Type$$endif$Array values;

    public $Type$BigArrayVector($if(boolean)$Bit$else$$Type$$endif$Array values, int positionCount) {
        super(positionCount);
        this.values = values;
    }

    @Override
    public $Type$Block asBlock() {
        return new $Type$VectorBlock(this);
    }

    @Override
    public $type$ get$Type$(int position) {
        return values.get(position);
    }

    @Override
    public ElementType elementType() {
        return ElementType.$TYPE$;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public $Type$Vector filter(int... positions) {
        return new Filter$Type$Vector(this, positions);
    }

    @Override
    public void close() {
        values.close();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof $Type$Vector that) {
            return $Type$Vector.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return $Type$Vector.hash(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", values=" + values + ']';
    }
}
