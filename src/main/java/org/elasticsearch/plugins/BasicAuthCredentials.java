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

package org.elasticsearch.plugins;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.Strings;

import java.net.URLConnection;

/**
 * This implementation provides basic HTTP Auth by adding
 * `Authorization: Basic (Base 64 encoded username:password)`
 * to the URLConnection.
 */
public class BasicAuthCredentials implements AuthCredentials {

    private final String username;
    private final String password;

    public BasicAuthCredentials(String username, String password) {
        if (!Strings.hasText(username) || !Strings.hasText(password)) {
            throw new ElasticsearchIllegalArgumentException("username or password is empty");
        }

        this.username = username;
        this.password = password;
    }

    @Override
    public void applyAuthorization(URLConnection connection) {
        connection.setRequestProperty("Authorization", "Basic " + Base64.encodeBytes( (username + ":" + password).getBytes() ));
    }
}
