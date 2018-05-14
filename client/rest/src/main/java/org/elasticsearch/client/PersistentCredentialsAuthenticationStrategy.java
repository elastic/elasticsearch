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
 *
 *
 */

package org.elasticsearch.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScheme;
import org.apache.http.impl.client.TargetAuthenticationStrategy;
import org.apache.http.protocol.HttpContext;

/**
 * An {@link org.apache.http.client.AuthenticationStrategy} implementation that does <em>not</em> perform
 * any special handling if authentication fails.
 * The default handler in Apache HTTP client mimics standard browser behaviour of clearing authentication
 * credentials if it receives a 401 response from the server. While this can be useful for browser, it is
 * rarely the desired behaviour with the Elasticsearch REST API.
 * When an Elasticsearch node starts up with X-Pack enabled, the standard behaviour is to respond with a
 * 401 status until sufficient state as been recovered for it to determine whether security is enabled and
 * what users exist.
 * The desired behaviour under these circumstances is for the Rest client to retry with the same credentials.
 */
class PersistentCredentialsAuthenticationStrategy extends TargetAuthenticationStrategy {

    private final Log logger = LogFactory.getLog(PersistentCredentialsAuthenticationStrategy.class);

    @Override
    public void authFailed(HttpHost host, AuthScheme authScheme, HttpContext context) {
        if (logger.isDebugEnabled()) {
            logger.debug("Authentication to " + host + " failed (scheme: " + authScheme.getSchemeName()
                + "). Preserving credentials for next request");
        }
        // Do nothing.
        // The superclass implementation of method will clear the credentials from the cache, but we don't
    }
}
