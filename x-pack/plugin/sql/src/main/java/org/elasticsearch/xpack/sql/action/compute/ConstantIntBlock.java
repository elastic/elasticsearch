/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute;

public class ConstantIntBlock extends Block {
    private final int constant;

    public ConstantIntBlock(int positionCount, int constant) {
        super(positionCount);
        this.constant = constant;
    }

    @Override
    public int getInt(int position) {
        return constant;
    }

    @Override
    public String toString() {
        return "ConstantIntBlock{" +
            "constant=" + constant +
            '}';
    }
}
