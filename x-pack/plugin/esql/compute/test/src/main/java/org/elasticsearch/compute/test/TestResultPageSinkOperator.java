/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.test;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.PageConsumerOperator;

import java.util.function.Consumer;

/**
 * Page Consumer operator that deep copies the input page, closes it, and then passes the copy
 * to the underlying page consumer.
 */
public class TestResultPageSinkOperator extends PageConsumerOperator {

    public TestResultPageSinkOperator(Consumer<Page> pageConsumer) {
        super(page -> {
            Page copy = BlockTestUtils.deepCopyOf(page, TestBlockFactory.getNonBreakingInstance());
            page.releaseBlocks();
            pageConsumer.accept(copy);
        });
    }
}
