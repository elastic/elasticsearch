/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.support;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.*;
import org.elasticsearch.util.Nullable;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.settings.Settings;

import static org.elasticsearch.action.support.PlainActionFuture.*;

/**
 * @author kimchy (Shay Banon)
 */
public abstract class BaseAction<Request extends ActionRequest, Response extends ActionResponse> extends AbstractComponent implements Action<Request, Response> {

    protected BaseAction(Settings settings) {
        super(settings);
    }

    @Override public ActionFuture<Response> submit(Request request) throws ElasticSearchException {
        return submit(request, null);
    }

    @Override public ActionFuture<Response> submit(Request request, @Nullable ActionListener<Response> listener) {
        PlainActionFuture<Response> future = newFuture(listener);
        if (listener == null) {
            // since we don't have a listener, and we release a possible lock with the future
            // there is no need to execute it under a listener thread
            request.listenerThreaded(false);
        }
        execute(request, future);
        return future;
    }

    @Override public void execute(Request request, ActionListener<Response> listener) {
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        doExecute(request, listener);
    }

    protected abstract void doExecute(Request request, ActionListener<Response> responseActionListener);
}
