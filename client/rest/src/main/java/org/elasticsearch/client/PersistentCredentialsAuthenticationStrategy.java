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
 * If the code using the REST client has configured credentials for the REST API, then we can and should
 * assume that this is intentional, and those credentials represent the best possible authentication
 * mechanism to the Elasticsearch node.
 * If we receive a 401 status, a probably cause is that the authentication mechanism in place was unable
 * to perform the requisite password checks (the node has not yet recovered its state, or an external
 * authentication provider was unavailable).
 * If this occurs, then the desired behaviour is for the Rest client to retry with the same credentials
 * (rather than trying with no credentials, or expecting the calling code to provide alternate credentials).
 */
final class PersistentCredentialsAuthenticationStrategy extends TargetAuthenticationStrategy {

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
