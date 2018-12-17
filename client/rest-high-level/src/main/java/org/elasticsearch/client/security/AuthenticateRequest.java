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

package org.elasticsearch.client.security;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Validatable;

/**
 * Empty request object required to make the authenticate call. The authenticate call
 * retrieves metadata about the authenticated user.
 */
public final class AuthenticateRequest implements Validatable {

    public static final AuthenticateRequest INSTANCE = new AuthenticateRequest();

    private AuthenticateRequest() {
    }

    public Request getRequest() {
        return new Request(HttpGet.METHOD_NAME, "/_security/_authenticate");
    }

}
