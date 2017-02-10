/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.FilterClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.crypto.CryptoService;
import org.elasticsearch.xpack.security.user.XPackUser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A special filter client for internal node communication which adds the internal xpack user to the headers.
 * An optionally secured client for internal node communication.
 *
 * When secured, the XPack user is added to the execution context before each action is executed.
 */
public class InternalClient extends FilterClient {

    private final CryptoService cryptoService;
    private final boolean signUserHeader;
    private final String nodeName;

    /**
     * Constructs an InternalClient.
     * If {@code cryptoService} is non-null, the client is secure. Otherwise this client is a passthrough.
     */
    public InternalClient(Settings settings, ThreadPool threadPool, Client in, CryptoService cryptoService) {
        super(settings, threadPool, in);
        this.cryptoService = cryptoService;
        this.signUserHeader = AuthenticationService.SIGN_USER_HEADER.get(settings);
        this.nodeName = Node.NODE_NAME_SETTING.get(settings);
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends
        ActionRequestBuilder<Request, Response, RequestBuilder>> void doExecute(
        Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {

        if (cryptoService == null) {
            super.doExecute(action, request, listener);
            return;
        }

        final ThreadContext threadContext = threadPool().getThreadContext();
        final Supplier<ThreadContext.StoredContext> storedContext = threadContext.newRestorableContext(true);
        // we need to preserve the context here otherwise we execute the response with the XPack user which we can cause problems
        // since we expect the callback to run with the authenticated user calling the doExecute method
        try (ThreadContext.StoredContext ctx = threadContext.stashContext()) {
            processContext(threadContext);
            super.doExecute(action, request, new ContextPreservingActionListener<>(storedContext, listener));
        }
    }

    protected void processContext(ThreadContext threadContext) {
        try {
            Authentication authentication = new Authentication(XPackUser.INSTANCE,
                    new Authentication.RealmRef("__attach", "__attach", nodeName), null);
            authentication.writeToContext(threadContext, cryptoService, signUserHeader);
        } catch (IOException ioe) {
            throw new ElasticsearchException("failed to attach internal user to request", ioe);
        }
    }

    /**
     * This method fetches all results for the given search request, parses them using the given hit parser and calls the
     * listener once done.
     */
    public static <T> void fetchAllByEntity(Client client, SearchRequest request, final ActionListener<Collection<T>> listener,
                                            Function<SearchHit, T> hitParser) {
        final List<T> results = new ArrayList<>();
        if (request.scroll() == null) { // we do scroll by default lets see if we can get rid of this at some point.
            request.scroll(TimeValue.timeValueSeconds(10L));
        }
        final Consumer<SearchResponse> clearScroll = (response) -> {
            if (response != null && response.getScrollId() != null) {
                ClearScrollRequest clearScrollRequest = client.prepareClearScroll().addScrollId(response.getScrollId()).request();
                client.clearScroll(clearScrollRequest, ActionListener.wrap((r) -> {}, (e) -> {}));
            }
        };
        // This function is MADNESS! But it works, don't think about it too hard...
        // simon edit: just watch this if you got this far https://www.youtube.com/watch?v=W-lF106Dgk8
        client.search(request, new ActionListener<SearchResponse>() {
            private volatile SearchResponse lastResponse = null;

            @Override
            public void onResponse(SearchResponse resp) {
                try {
                    lastResponse = resp;
                    if (resp.getHits().getHits().length > 0) {
                        for (SearchHit hit : resp.getHits().getHits()) {
                            final T oneResult = hitParser.apply(hit);
                            if (oneResult != null) {
                                results.add(oneResult);
                            }
                        }
                        SearchScrollRequest scrollRequest = client.prepareSearchScroll(resp.getScrollId())
                                .setScroll(request.scroll().keepAlive()).request();
                        client.searchScroll(scrollRequest, this);

                    } else {
                        clearScroll.accept(resp);
                        // Finally, return the list of users
                        listener.onResponse(Collections.unmodifiableList(results));
                    }
                } catch (Exception e){
                    onFailure(e); // lets clean up things
                }
            }

            @Override
            public void onFailure(Exception t) {
                try {
                    // attempt to clear the scroll request
                    clearScroll.accept(lastResponse);
                } finally {
                    if (t instanceof IndexNotFoundException) {
                        // since this is expected to happen at times, we just call the listener with an empty list
                        listener.onResponse(Collections.<T>emptyList());
                    } else {
                        listener.onFailure(t);
                    }
                }
            }
        });
    }
}
