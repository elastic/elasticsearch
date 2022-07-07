/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.action.compute.Driver;
import org.elasticsearch.xpack.sql.action.compute.LongBlock;
import org.elasticsearch.xpack.sql.action.compute.LongGroupingOperator;
import org.elasticsearch.xpack.sql.action.compute.LongMaxOperator;
import org.elasticsearch.xpack.sql.action.compute.LongTransformer;
import org.elasticsearch.xpack.sql.action.compute.Operator;
import org.elasticsearch.xpack.sql.action.compute.Page;
import org.elasticsearch.xpack.sql.action.compute.PageConsumerOperator;

import java.util.List;

public class OperatorTests extends ESTestCase {

    class RandomLongBlockSourceOperator implements Operator {

        boolean finished;

        @Override
        public Page getOutput() {
            if (random().nextInt(100) < 1) {
                finish();
            }
            final int size = randomIntBetween(1, 10);
            final long[] array = new long[size];
            for (int i = 0; i < array.length; i++) {
                array[i] = randomLongBetween(0, 5);
            }
            return new Page(new LongBlock(array, array.length));
        }

        @Override
        public boolean isFinished() {
            return finished;
        }

        @Override
        public void finish() {
            finished = true;
        }

        @Override
        public boolean needsInput() {
            return false;
        }

        @Override
        public void addInput(Page page) {
            throw new UnsupportedOperationException();
        }
    }

    public void testOperators() {
        Driver driver = new Driver(List.of(
            new RandomLongBlockSourceOperator(),
            new LongTransformer(0, i -> i + 1),
            new LongGroupingOperator(1, BigArrays.NON_RECYCLING_INSTANCE),
            new LongMaxOperator(2),
            new PageConsumerOperator(page -> logger.info("New block: {}", page))),
            () -> {});
        driver.run();
    }
}
