/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.support.replication;

import org.elasticsearch.action.ActionListener;

import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

public abstract class TransportWriteActionTestHelper {

    private static void waitForPostWriteActions(Consumer<ActionListener> listenerConsumer) {
        final CountDownLatch latch = new CountDownLatch(1);
        final ActionListener listener = new ActionListener() {
            @Override
            public void onResponse(Object o) {
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        };
        listenerConsumer.accept(listener);
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    public static void waitForPostWriteActions(final TransportWriteAction.WritePrimaryResult<?, ?> result) {
        waitForPostWriteActions(listener -> result.respond(listener));
    }

    public static void waitForPostWriteActions(TransportWriteAction.WriteReplicaResult<?> result) {
        waitForPostWriteActions(listener -> result.respond(listener));
    }
}
