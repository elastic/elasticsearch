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

/**
 * An indexing listener for indexing, delete, events.
 */
public interface IndexingOperationListener {

    /**
     * Called before the indexing occurs.
     */
    default Engine.Index preIndex(Engine.Index operation) {
        return operation;
    }

    /**
     * Called after the indexing operation occurred.
     */
    default void postIndex(Engine.Index index, boolean created) {}

    /**
     * Called after the indexing operation occurred with exception.
     */
    default void postIndex(Engine.Index index, Exception ex) {}

    /**
     * Called before the delete occurs.
     */
    default Engine.Delete preDelete(Engine.Delete delete) {
        return delete;
    }


    /**
     * Called after the delete operation occurred.
     */
    default void postDelete(Engine.Delete delete) {}

    /**
     * Called after the delete operation occurred with exception.
     */
    default void postDelete(Engine.Delete delete, Exception ex) {}

    /**
     * A Composite listener that multiplexes calls to each of the listeners methods.
     */
    final class CompositeListener implements IndexingOperationListener{
        private final List<IndexingOperationListener> listeners;
        private final Logger logger;

        public CompositeListener(List<IndexingOperationListener> listeners, Logger logger) {
            this.listeners = listeners;
            this.logger = logger;
        }

        @Override
        public Engine.Index preIndex(Engine.Index operation) {
            assert operation != null;
            for (IndexingOperationListener listener : listeners) {
                try {
                    listener.preIndex(operation);
                } catch (Exception e) {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("preIndex listener [{}] failed", listener), e);
                }
            }
            return operation;
        }

        @Override
        public void postIndex(Engine.Index index, boolean created) {
            assert index != null;
            for (IndexingOperationListener listener : listeners) {
                try {
                    listener.postIndex(index, created);
                } catch (Exception e) {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("postIndex listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void postIndex(Engine.Index index, Exception ex) {
            assert index != null && ex != null;
            for (IndexingOperationListener listener : listeners) {
                try {
                    listener.postIndex(index, ex);
                } catch (Exception inner) {
                    inner.addSuppressed(ex);
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("postIndex listener [{}] failed", listener), inner);
                }
            }
        }

        @Override
        public Engine.Delete preDelete(Engine.Delete delete) {
            assert delete != null;
            for (IndexingOperationListener listener : listeners) {
                try {
                    listener.preDelete(delete);
                } catch (Exception e) {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("preDelete listener [{}] failed", listener), e);
                }
            }
            return delete;
        }

        @Override
        public void postDelete(Engine.Delete delete) {
            assert delete != null;
            for (IndexingOperationListener listener : listeners) {
                try {
                    listener.postDelete(delete);
                } catch (Exception e) {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("postDelete listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void postDelete(Engine.Delete delete, Exception ex) {
            assert delete != null && ex != null;
            for (IndexingOperationListener listener : listeners) {
                try {
                    listener.postDelete(delete, ex);
                } catch (Exception inner) {
                    inner.addSuppressed(ex);
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("postDelete listener [{}] failed", listener), inner);
                }
            }
        }
    }
}
