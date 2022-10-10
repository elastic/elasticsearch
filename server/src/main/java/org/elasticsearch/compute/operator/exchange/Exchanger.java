/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Operator;

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
            return Operator.NOT_BLOCKED;
        }
    };
}
