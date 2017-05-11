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

package org.elasticsearch.rest.action;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;

/**
 * An action listener that requires {@link #processResponse(Object)} to be implemented
 * and will automatically handle failures.
 */
public abstract class RestActionListener<Response> implements ActionListener<Response> {

    // we use static here so we won't have to pass the actual logger each time for a very rare case of logging
    // where the settings don't matter that much
    private static Logger logger = Loggers.getLogger(RestResponseListener.class);

    protected final RestChannel channel;

    protected RestActionListener(RestChannel channel) {
        this.channel = channel;
    }

    @Override
    public final void onResponse(Response response) {
        try {
            processResponse(response);
        } catch (Exception e) {
            onFailure(e);
        }
    }

    protected abstract void processResponse(Response response) throws Exception;

    @Override
    public final void onFailure(Exception e) {
        try {
            channel.sendResponse(new BytesRestResponse(channel, e));
        } catch (Exception inner) {
            inner.addSuppressed(e);
            logger.error("failed to send failure response", inner);
        }
    }
}
