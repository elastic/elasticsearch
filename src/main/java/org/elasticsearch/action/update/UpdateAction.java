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

package org.elasticsearch.action.update;

import org.elasticsearch.action.ClientAction;
import org.elasticsearch.client.Client;

/**
 */
public class UpdateAction extends ClientAction<UpdateRequest, UpdateResponse, UpdateRequestBuilder> {

    public static final UpdateAction INSTANCE = new UpdateAction();
    public static final String NAME = "indices:data/write/update";

    private UpdateAction() {
        super(NAME);
    }

    @Override
    public UpdateResponse newResponse() {
        return new UpdateResponse();
    }

    @Override
    public UpdateRequestBuilder newRequestBuilder(Client client) {
        return new UpdateRequestBuilder(client);
    }
}
