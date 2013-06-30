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

package org.elasticsearch.action.terms;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.TransportRequestOptions;

/**
 * The action to request terms by query
 */
public class TermsByQueryAction extends Action<TermsByQueryRequest, TermsByQueryResponse, TermsByQueryRequestBuilder> {

    public static final TermsByQueryAction INSTANCE = new TermsByQueryAction();
    public static final String NAME = "termsbyquery";

    /**
     * Default constructor
     */
    private TermsByQueryAction() {
        super(NAME);
    }

    /**
     * Gets a new {@link TermsByQueryResponse} object
     *
     * @return the new {@link TermsByQueryResponse}.
     */
    @Override
    public TermsByQueryResponse newResponse() {
        return new TermsByQueryResponse();
    }

    /**
     * Set transport options specific to a terms by query request.
     *
     * @param settings node settings
     * @return the request options.
     */
    @Override
    public TransportRequestOptions transportOptions(Settings settings) {
        TransportRequestOptions opts = new TransportRequestOptions();
        opts.withType(TransportRequestOptions.Type.BULK);  // TODO: just stick with default of REG?
        opts.withCompress(true);

        // return TransportRequestOptions.EMPTY;
        return opts;
    }

    /**
     * Get a new {@link TermsByQueryRequestBuilder}
     *
     * @param client the client responsible for executing the request.
     * @return the new {@link TermsByQueryRequestBuilder}
     */
    @Override
    public TermsByQueryRequestBuilder newRequestBuilder(Client client) {
        return new TermsByQueryRequestBuilder(client);
    }
}
