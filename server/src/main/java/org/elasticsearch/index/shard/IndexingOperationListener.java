/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.shard;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.IndexOperationBatch;

import java.util.List;

/**
 * An indexing listener for indexing, delete, events.
 */
public interface IndexingOperationListener {

    /**
     * Called before the indexing occurs.
     */
    default Engine.Index preIndex(ShardId shardId, Engine.Index operation) {
        return operation;
    }

    /**
     * Called after the indexing operation occurred. Note that this is
     * also called when indexing a document did not succeed due to document
     * related failures. See {@link #postIndex(ShardId, Engine.Index, Exception)}
     * for engine level failures
     */
    default void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {}

    /**
     * Called after the indexing operation occurred with engine level exception.
     * See {@link #postIndex(ShardId, Engine.Index, Engine.IndexResult)} for document
     * related failures
     */
    default void postIndex(ShardId shardId, Engine.Index index, Exception ex) {}

    /**
     * Batch variant of {@link #preIndex}. The default delegates to {@link #preIndex} per operation.
     */
    default IndexOperationBatch preIndexBatch(ShardId shardId, IndexOperationBatch batch) {
        for (Engine.Index operation : batch.materializeIndexOps()) {
            preIndex(shardId, operation);
        }
        return batch;
    }

    /**
     * Batch variant of {@link #postIndex(ShardId, Engine.Index, Engine.IndexResult)}.
     * See {@link #postIndexBatch(ShardId, IndexOperationBatch, Exception)} for
     * engine level failures.
     */
    default void postIndexBatch(ShardId shardId, IndexOperationBatch batch, List<Engine.IndexResult> results) {
        final List<Engine.Index> operations = batch.materializeIndexOps();
        for (int i = 0; i < results.size(); i++) {
            postIndex(shardId, operations.get(i), results.get(i));
        }
    }

    /**
     * Batch variant of {@link #postIndex(ShardId, Engine.Index, Exception)}.
     * See {@link #postIndexBatch(ShardId, IndexOperationBatch, List)} for document related failures.
     */
    default void postIndexBatch(ShardId shardId, IndexOperationBatch batch, Exception ex) {
        for (Engine.Index operation : batch.materializeIndexOps()) {
            postIndex(shardId, operation, ex);
        }
    }

    /**
     * Called before the delete occurs.
     */
    default Engine.Delete preDelete(ShardId shardId, Engine.Delete delete) {
        return delete;
    }

    /**
     * Called after the delete operation occurred. Note that this is
     * also called when deleting a document did not succeed due to document
     * related failures. See {@link #postDelete(ShardId, Engine.Delete, Exception)}
     * for engine level failures
     */
    default void postDelete(ShardId shardId, Engine.Delete delete, Engine.DeleteResult result) {}

    /**
     * Called after the delete operation occurred with engine level exception.
     * See {@link #postDelete(ShardId, Engine.Delete, Engine.DeleteResult)} for document
     * related failures
     */
    default void postDelete(ShardId shardId, Engine.Delete delete, Exception ex) {}

    /**
     * A Composite listener that multiplexes calls to each of the listeners methods.
     */
    final class CompositeListener implements IndexingOperationListener {
        private final List<IndexingOperationListener> listeners;
        private final Logger logger;

        public CompositeListener(List<IndexingOperationListener> listeners, Logger logger) {
            this.listeners = listeners;
            this.logger = logger;
        }

        @Override
        public Engine.Index preIndex(ShardId shardId, Engine.Index operation) {
            assert operation != null;
            for (IndexingOperationListener listener : listeners) {
                try {
                    listener.preIndex(shardId, operation);
                } catch (Exception e) {
                    logger.warn(() -> "preIndex listener [" + listener + "] failed", e);
                }
            }
            return operation;
        }

        @Override
        public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
            assert index != null;
            for (IndexingOperationListener listener : listeners) {
                try {
                    listener.postIndex(shardId, index, result);
                } catch (Exception e) {
                    logger.warn(() -> "postIndex listener [" + listener + "] failed", e);
                }
            }
        }

        @Override
        public void postIndex(ShardId shardId, Engine.Index index, Exception ex) {
            assert index != null && ex != null;
            for (IndexingOperationListener listener : listeners) {
                try {
                    listener.postIndex(shardId, index, ex);
                } catch (Exception inner) {
                    inner.addSuppressed(ex);
                    logger.warn(() -> "postIndex listener [" + listener + "] failed", inner);
                }
            }
        }

        @Override
        public IndexOperationBatch preIndexBatch(ShardId shardId, IndexOperationBatch batch) {
            assert batch != null;
            for (IndexingOperationListener listener : listeners) {
                try {
                    listener.preIndexBatch(shardId, batch);
                } catch (Exception e) {
                    logger.warn(() -> "preIndexBatch listener [" + listener + "] failed", e);
                }
            }
            return batch;
        }

        @Override
        public void postIndexBatch(ShardId shardId, IndexOperationBatch batch, List<Engine.IndexResult> results) {
            assert batch != null && results != null;
            for (IndexingOperationListener listener : listeners) {
                try {
                    listener.postIndexBatch(shardId, batch, results);
                } catch (Exception e) {
                    logger.warn(() -> "postIndexBatch listener [" + listener + "] failed", e);
                }
            }
        }

        @Override
        public void postIndexBatch(ShardId shardId, IndexOperationBatch batch, Exception ex) {
            assert batch != null && ex != null;
            for (IndexingOperationListener listener : listeners) {
                try {
                    listener.postIndexBatch(shardId, batch, ex);
                } catch (Exception inner) {
                    inner.addSuppressed(ex);
                    logger.warn(() -> "postIndexBatch listener [" + listener + "] failed", inner);
                }
            }
        }

        @Override
        public Engine.Delete preDelete(ShardId shardId, Engine.Delete delete) {
            assert delete != null;
            for (IndexingOperationListener listener : listeners) {
                try {
                    listener.preDelete(shardId, delete);
                } catch (Exception e) {
                    logger.warn(() -> "preDelete listener [" + listener + "] failed", e);
                }
            }
            return delete;
        }

        @Override
        public void postDelete(ShardId shardId, Engine.Delete delete, Engine.DeleteResult result) {
            assert delete != null;
            for (IndexingOperationListener listener : listeners) {
                try {
                    listener.postDelete(shardId, delete, result);
                } catch (Exception e) {
                    logger.warn(() -> "postDelete listener [" + listener + "] failed", e);
                }
            }
        }

        @Override
        public void postDelete(ShardId shardId, Engine.Delete delete, Exception ex) {
            assert delete != null && ex != null;
            for (IndexingOperationListener listener : listeners) {
                try {
                    listener.postDelete(shardId, delete, ex);
                } catch (Exception inner) {
                    inner.addSuppressed(ex);
                    logger.warn(() -> "postDelete listener [" + listener + "] failed", inner);
                }
            }
        }
    }
}
