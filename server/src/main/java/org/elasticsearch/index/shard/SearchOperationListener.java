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
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.transport.TransportRequest;

import java.util.List;

/**
 * An listener for search, fetch and context events.
 */
public interface SearchOperationListener {

    /**
     * Executed before the query phase is executed
     * @param searchContext the current search context
     */
    default void onPreQueryPhase(SearchContext searchContext) {}

    /**
     * Executed if a query phased failed.
     * @param searchContext the current search context
     */
    default void onFailedQueryPhase(SearchContext searchContext) {}

    /**
     * Executed after the query phase successfully finished.
     * Note: this is not invoked if the query phase execution failed.
     * @param searchContext the current search context
     * @param tookInNanos the number of nanoseconds the query execution took
     *
     * @see #onFailedQueryPhase(SearchContext)
     */
    default void onQueryPhase(SearchContext searchContext, long tookInNanos) {}

    /**
     * Executed before the fetch phase is executed
     * @param searchContext the current search context
     */
    default void onPreFetchPhase(SearchContext searchContext) {}

    /**
     * Executed if a fetch phased failed.
     * @param searchContext the current search context
     */
    default void onFailedFetchPhase(SearchContext searchContext) {}

    /**
     * Executed after the fetch phase successfully finished.
     * Note: this is not invoked if the fetch phase execution failed.
     * @param searchContext the current search context
     * @param tookInNanos the number of nanoseconds the fetch execution took
     *
     * @see #onFailedFetchPhase(SearchContext)
     */
    default void onFetchPhase(SearchContext searchContext, long tookInNanos) {}

    /**
     * Executed when a new reader context was created
     * @param readerContext the created context
     */
    default void onNewReaderContext(ReaderContext readerContext) {}

    /**
     * Executed when a previously created reader context is freed.
     * This happens either when the search execution finishes, if the
     * execution failed or if the search context as idle for and needs to be
     * cleaned up.
     * @param readerContext the freed reader context
     */
    default void onFreeReaderContext(ReaderContext readerContext) {}

    /**
     * Executed when a new scroll search {@link ReaderContext} was created
     * @param readerContext the created reader context
     */
    default void onNewScrollContext(ReaderContext readerContext) {}

    /**
     * Executed when a scroll search {@link SearchContext} is freed.
     * This happens either when the scroll search execution finishes, if the
     * execution failed or if the search context as idle for and needs to be
     * cleaned up.
     * @param readerContext the freed search context
     */
    default void onFreeScrollContext(ReaderContext readerContext) {}

    /**
     * Executed prior to using a {@link ReaderContext} that has been retrieved
     * from the active contexts. If the context is deemed invalid a runtime
     * exception can be thrown, which will prevent the context from being used.
     * @param readerContext The reader context used by this request.
     * @param transportRequest the request that is going to use the search context
     */
    default void validateReaderContext(ReaderContext readerContext, TransportRequest transportRequest) {}

    /**
     * A Composite listener that multiplexes calls to each of the listeners methods.
     */
    final class CompositeListener implements SearchOperationListener {
        private final SearchOperationListener[] listeners;
        private final Logger logger;

        CompositeListener(List<SearchOperationListener> listeners, Logger logger) {
            this.listeners = listeners.toArray(new SearchOperationListener[0]);
            this.logger = logger;
        }

        @Override
        public void onPreQueryPhase(SearchContext searchContext) {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onPreQueryPhase(searchContext);
                } catch (Exception e) {
                    logger.warn(() -> "onPreQueryPhase listener [" + listener + "] failed", e);
                }
            }
        }

        @Override
        public void onFailedQueryPhase(SearchContext searchContext) {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onFailedQueryPhase(searchContext);
                } catch (Exception e) {
                    logger.warn(() -> "onFailedQueryPhase listener [" + listener + "] failed", e);
                }
            }
        }

        @Override
        public void onQueryPhase(SearchContext searchContext, long tookInNanos) {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onQueryPhase(searchContext, tookInNanos);
                } catch (Exception e) {
                    logger.warn(() -> "onQueryPhase listener [" + listener + "] failed", e);
                }
            }
        }

        @Override
        public void onPreFetchPhase(SearchContext searchContext) {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onPreFetchPhase(searchContext);
                } catch (Exception e) {
                    logger.warn(() -> "onPreFetchPhase listener [" + listener + "] failed", e);
                }
            }
        }

        @Override
        public void onFailedFetchPhase(SearchContext searchContext) {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onFailedFetchPhase(searchContext);
                } catch (Exception e) {
                    logger.warn(() -> "onFailedFetchPhase listener [" + listener + "] failed", e);
                }
            }
        }

        @Override
        public void onFetchPhase(SearchContext searchContext, long tookInNanos) {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onFetchPhase(searchContext, tookInNanos);
                } catch (Exception e) {
                    logger.warn(() -> "onFetchPhase listener [" + listener + "] failed", e);
                }
            }
        }

        @Override
        public void onNewReaderContext(ReaderContext readerContext) {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onNewReaderContext(readerContext);
                } catch (Exception e) {
                    logger.warn(() -> "onNewContext listener [" + listener + "] failed", e);
                }
            }
        }

        @Override
        public void onFreeReaderContext(ReaderContext readerContext) {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onFreeReaderContext(readerContext);
                } catch (Exception e) {
                    logger.warn(() -> "onFreeContext listener [" + listener + "] failed", e);
                }
            }
        }

        @Override
        public void onNewScrollContext(ReaderContext readerContext) {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onNewScrollContext(readerContext);
                } catch (Exception e) {
                    logger.warn(() -> "onNewScrollContext listener [" + listener + "] failed", e);
                }
            }
        }

        @Override
        public void onFreeScrollContext(ReaderContext readerContext) {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onFreeScrollContext(readerContext);
                } catch (Exception e) {
                    logger.warn(() -> "onFreeScrollContext listener [" + listener + "] failed", e);
                }
            }
        }

        @Override
        public void validateReaderContext(ReaderContext readerContext, TransportRequest request) {
            Exception exception = null;
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.validateReaderContext(readerContext, request);
                } catch (Exception e) {
                    exception = ExceptionsHelper.useOrSuppress(exception, e);
                }
            }
            ExceptionsHelper.reThrowIfNotNull(exception);
        }
    }
}
