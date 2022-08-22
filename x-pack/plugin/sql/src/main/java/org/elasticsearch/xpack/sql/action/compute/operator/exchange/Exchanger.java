/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.operator.exchange;

import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.xpack.sql.action.compute.data.Page;

import static org.elasticsearch.xpack.sql.action.compute.operator.Operator.NOT_BLOCKED;

/**
 * Exchangers provide different means for handing off data to exchange sources, e.g. allow multiplexing.
 */
public interface Exchanger {

    void accept(Page page);

    default void finish() {

    }

    ListenableActionFuture<Void> waitForWriting();

    Exchanger FINISHED = new Exchanger() {
        @Override
        public void accept(Page page) {}

        @Override
        public ListenableActionFuture<Void> waitForWriting() {
            return NOT_BLOCKED;
        }
    };
}
