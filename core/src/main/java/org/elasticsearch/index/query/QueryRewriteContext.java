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
package org.elasticsearch.index.query;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

/**
 * Context object used to rewrite {@link QueryBuilder} instances into simplified version.
 */
public class QueryRewriteContext {
    private final NamedXContentRegistry xContentRegistry;
    protected final Client client;
    protected final LongSupplier nowInMillis;
    private final List<BiConsumer<Client, ActionListener>> asyncActions = new ArrayList<>();


    public QueryRewriteContext(NamedXContentRegistry xContentRegistry, Client client, LongSupplier nowInMillis) {
        this.xContentRegistry = xContentRegistry;
        this.client = client;
        this.nowInMillis = nowInMillis;
    }

    /**
     * The registry used to build new {@link XContentParser}s. Contains registered named parsers needed to parse the query.
     */
    public NamedXContentRegistry getXContentRegistry() {
        return xContentRegistry;
    }

    public long nowInMillis() {
        return nowInMillis.getAsLong();
    }

    /**
     * Returns an instance of {@link QueryShardContext} if available of null otherwise
     */
    public QueryShardContext convertToShardContext() {
        return null;
    }

    public void registerAsyncAction(BiConsumer<Client, ActionListener> asyncAction) {
        asyncActions.add(asyncAction);
    }

    public boolean hasAsyncActions() {
        return asyncActions.isEmpty() == false;
    }

    public void executeAsyncActions(ActionListener listener) {
        if (asyncActions.isEmpty()) {
            listener.onResponse(null);
        } else {
            CountDown done = new CountDown(asyncActions.size());
            ActionListener internalListener = new ActionListener() {
                @Override
                public void onResponse(Object o) {
                    if (done.countDown()) {
                        listener.onResponse(null);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    if (done.fastForward()) {
                        listener.onFailure(e);
                    }
                }
            };
            ArrayList<BiConsumer<Client, ActionListener>> biConsumers = new ArrayList<>(asyncActions);
            asyncActions.clear();
            for (BiConsumer<Client, ActionListener> action : biConsumers) {
                action.accept(client, internalListener);
            }
        }
    }

}
