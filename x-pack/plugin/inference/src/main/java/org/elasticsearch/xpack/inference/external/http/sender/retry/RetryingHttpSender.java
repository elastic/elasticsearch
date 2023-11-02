/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender.retry;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderFactory;

public class RetryingHttpSender extends HttpRequestSenderFactory.HttpRequestSender {
    public RetryingHttpSender(
        String serviceName,
        ThreadPool threadPool,
        HttpClientManager httpClientManager,
        ClusterService clusterService,
        Settings settings
    ) {
        super(serviceName, threadPool, httpClientManager, clusterService, settings);
    }

    /*
    Thoughts:
    1. It'd probably be nice if we still adhered to the Sender contract?
    2. Maybe this class would provide functionality to wrap a ActionListener<HttpResult>
        That way the original service client could handle the parsing of the results but if
        that fails for some reason it would auto call into this class without having to explicitly call some function on failure
    3. This class could have default functionality on how to handle error status codes but it should be overridable somehow
        Kind of like, we could pass in a function that would handle error checking to override the default behavior
    4. Should the retryer have the same configurations across all clients? If not then we'll need to construct them per client
        That's probably easy, just like we pass in a serviceName when constructing the current sender we can pass in those options maybe
    5. Maybe we need an interface that extends Sender that adds a new send method that allows passing in logic for how to handle failures
        So instead of it passing in a ActionListener<HttpResult> listener, it would pass in a listener that has the inference results or
        something else?
    6. We could pass in a class that would define how to handle errors, and how to do the parsing
     */
}
