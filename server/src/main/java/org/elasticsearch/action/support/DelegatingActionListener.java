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

/*
 * A Simple class to handle wrapping a response with another response
 */
package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;

public abstract class DelegatingActionListener<Instigator extends ActionResponse, Delegated extends  ActionResponse> implements ActionListener<Instigator> {

    ActionListener<Delegated> delegatedActionListener;

    protected DelegatingActionListener(final ActionListener<Delegated> listener) {
        this.delegatedActionListener = listener;
    }

    protected abstract Delegated getDelegatedFromInstigator(Instigator response);

    @Override
    public final void onResponse(Instigator response) {
        delegatedActionListener.onResponse(getDelegatedFromInstigator(response));
    }

    @Override
    public final void onFailure(Exception e) {
        delegatedActionListener.onFailure(e);
    }
}
