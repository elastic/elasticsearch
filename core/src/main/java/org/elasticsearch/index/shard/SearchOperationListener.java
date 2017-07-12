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
import org.elasticsearch.ExceptionsHelper;
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
    default void onPreQueryPhase(SearchContext searchContext) {};

    /**
     * Executed if a query phased failed.
     * @param searchContext the current search context
     */
    default void onFailedQueryPhase(SearchContext searchContext) {};

    /**
     * Executed after the query phase successfully finished.
     * Note: this is not invoked if the query phase execution failed.
     * @param searchContext the current search context
     * @param tookInNanos the number of nanoseconds the query execution took
     *
     * @see #onFailedQueryPhase(SearchContext)
     */
    default void onQueryPhase(SearchContext searchContext, long tookInNanos) {};

    /**
     * Executed before the fetch phase is executed
     * @param searchContext the current search context
     */
    default void onPreFetchPhase(SearchContext searchContext) {};

    /**
     * Executed if a fetch phased failed.
     * @param searchContext the current search context
     */
    default void onFailedFetchPhase(SearchContext searchContext) {};

    /**
     * Executed after the fetch phase successfully finished.
     * Note: this is not invoked if the fetch phase execution failed.
     * @param searchContext the current search context
     * @param tookInNanos the number of nanoseconds the fetch execution took
     *
     * @see #onFailedFetchPhase(SearchContext)
     */
    default void onFetchPhase(SearchContext searchContext, long tookInNanos) {};

    /**
     * Executed when a new search context was created
     * @param context the created context
     */
    default void onNewContext(SearchContext context) {};

    /**
     * Executed when a previously created search context is freed.
     * This happens either when the search execution finishes, if the
     * execution failed or if the search context as idle for and needs to be
     * cleaned up.
     * @param context the freed search context
     */
    default void onFreeContext(SearchContext context) {};

    /**
     * Executed when a new scroll search {@link SearchContext} was created
     * @param context the created search context
     */
    default void onNewScrollContext(SearchContext context) {};

    /**
     * Executed when a scroll search {@link SearchContext} is freed.
     * This happens either when the scroll search execution finishes, if the
     * execution failed or if the search context as idle for and needs to be
     * cleaned up.
     * @param context the freed search context
     */
    default void onFreeScrollContext(SearchContext context) {};

    /**
     * Executed prior to using a {@link SearchContext} that has been retrieved
     * from the active contexts. If the context is deemed invalid a runtime
     * exception can be thrown, which will prevent the context from being used.
     * @param context the context retrieved from the active contexts
     * @param transportRequest the request that is going to use the search context
     */
    default void validateSearchContext(SearchContext context, TransportRequest transportRequest) {}

    /**
     * A Composite listener that multiplexes calls to each of the listeners methods.
     */
    final class CompositeListener implements SearchOperationListener {
        private final List<SearchOperationListener> listeners;
        private final Logger logger;

        public CompositeListener(List<SearchOperationListener> listeners, Logger logger) {
            this.listeners = listeners;
            this.logger = logger;
        }

        @Override
        public void onPreQueryPhase(SearchContext searchContext) {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onPreQueryPhase(searchContext);
                } catch (Exception e) {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("onPreQueryPhase listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onFailedQueryPhase(SearchContext searchContext) {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onFailedQueryPhase(searchContext);
                } catch (Exception e) {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("onFailedQueryPhase listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onQueryPhase(SearchContext searchContext, long tookInNanos) {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onQueryPhase(searchContext, tookInNanos);
                } catch (Exception e) {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("onQueryPhase listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onPreFetchPhase(SearchContext searchContext) {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onPreFetchPhase(searchContext);
                } catch (Exception e) {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("onPreFetchPhase listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onFailedFetchPhase(SearchContext searchContext) {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onFailedFetchPhase(searchContext);
                } catch (Exception e) {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("onFailedFetchPhase listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onFetchPhase(SearchContext searchContext, long tookInNanos) {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onFetchPhase(searchContext, tookInNanos);
                } catch (Exception e) {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("onFetchPhase listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onNewContext(SearchContext context) {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onNewContext(context);
                } catch (Exception e) {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("onNewContext listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onFreeContext(SearchContext context) {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onFreeContext(context);
                } catch (Exception e) {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("onFreeContext listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onNewScrollContext(SearchContext context) {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onNewScrollContext(context);
                } catch (Exception e) {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("onNewScrollContext listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onFreeScrollContext(SearchContext context) {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onFreeScrollContext(context);
                } catch (Exception e) {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("onFreeScrollContext listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void validateSearchContext(SearchContext context, TransportRequest request) {
            Exception exception = null;
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.validateSearchContext(context, request);
                } catch (Exception e) {
                    exception = ExceptionsHelper.useOrSuppress(exception, e);
                }
            }
            ExceptionsHelper.reThrowIfNotNull(exception);
        }
    }
}
