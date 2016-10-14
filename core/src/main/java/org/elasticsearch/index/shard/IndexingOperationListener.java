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
package org.elasticsearch.index.shard;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.index.engine.Engine;

import java.util.List;

/** An engine operation listener for index and delete execution. */
public interface IndexingOperationListener {

    /** Called before executing index or delete operation */
    default void preOperation(Engine.Operation operation) {}

    /** Called after executing index or delete operation */
    default void postOperation(Engine.Operation operation) {}

    /** Called after index or delete operation failed with exception */
    default void postOperation(Engine.Operation operation, Exception ex) {}

    /** A Composite listener that multiplexes calls to each of the listeners methods. */
    final class CompositeListener implements IndexingOperationListener{
        private final List<IndexingOperationListener> listeners;
        private final Logger logger;

        public CompositeListener(List<IndexingOperationListener> listeners, Logger logger) {
            this.listeners = listeners;
            this.logger = logger;
        }

        @Override
        public void preOperation(Engine.Operation operation) {
            assert operation != null;
            for (IndexingOperationListener listener : listeners) {
                try {
                    listener.preOperation(operation);
                } catch (Exception e) {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("preOperation listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void postOperation(Engine.Operation operation) {
            assert operation != null;
            for (IndexingOperationListener listener : listeners) {
                try {
                    listener.postOperation(operation);
                } catch (Exception e) {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("postOperation listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void postOperation(Engine.Operation operation, Exception ex) {
            assert operation != null && ex != null;
            for (IndexingOperationListener listener : listeners) {
                try {
                    listener.postOperation(operation, ex);
                } catch (Exception inner) {
                    inner.addSuppressed(ex);
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("postOperation listener [{}] failed", listener), inner);
                }
            }
        }
    }
}