/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.ClientHelper;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.security.authc.AuthenticationField;
import org.elasticsearch.xpack.security.authc.AuthenticationServiceField;

import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A helper class for actions which decides if we should run via the _xpack user and set ML as origin
 * or if we should use the run_as functionality by setting the correct headers
 */
public class MlClientHelper {

    /**
     * List of headers that are related to security
     */
    public static final Set<String> SECURITY_HEADER_FILTERS = Sets.newHashSet(AuthenticationServiceField.RUN_AS_USER_HEADER,
            AuthenticationField.AUTHENTICATION_KEY);

    /**
     * Execute a client operation and return the response, try to run a datafeed search with least privileges, when headers exist
     *
     * @param datafeedConfig The config for a datafeed
     * @param client         The client used to query
     * @param supplier       The action to run
     * @return               An instance of the response class
     */
    public static <T extends ActionResponse> T execute(DatafeedConfig datafeedConfig, Client client, Supplier<T> supplier) {
        return execute(datafeedConfig.getHeaders(), client, supplier);
    }

    /**
     * Execute a client operation and return the response, try to run an action with least privileges, when headers exist
     *
     * @param headers  Request headers, ideally including security headers
     * @param client   The client used to query
     * @param supplier The action to run
     * @return         An instance of the response class
     */
    public static <T extends ActionResponse> T execute(Map<String, String> headers, Client client, Supplier<T> supplier) {
        // no headers, we will have to use the xpack internal user for our execution by specifying the ml origin
        if (headers == null || headers.isEmpty()) {
            try (ThreadContext.StoredContext ignore = ClientHelper.stashWithOrigin(client.threadPool().getThreadContext(),
                    ClientHelper.ML_ORIGIN)) {
                return supplier.get();
            }
        } else {
            try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashContext()) {
                Map<String, String> filteredHeaders = headers.entrySet().stream()
                        .filter(e -> SECURITY_HEADER_FILTERS.contains(e.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                client.threadPool().getThreadContext().copyHeaders(filteredHeaders.entrySet());
                return supplier.get();
            }
        }
    }
}
