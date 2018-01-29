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

package org.elasticsearch.action.admin.cluster.reinit;

import org.elasticsearch.action.support.nodes.NodesOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;


/**
 * Builder for a cluster update settings request
 */
public class NodesReInitRequestBuilder
        extends NodesOperationRequestBuilder<NodesReInitRequest, NodesReInitResponse, NodesReInitRequestBuilder> {

    public NodesReInitRequestBuilder(ElasticsearchClient client, NodesReInitAction action) {
        super(client, action, new NodesReInitRequest());
    }

    /**
     * Sets the transient settings to be updated. They will not survive a full cluster restart
     */
    public NodesReInitRequestBuilder setSecureStorePassword(String secureStorePassword) {
        request.secureStorePassword(secureStorePassword);
        return this;
    }
//
//    /**
//     * Sets the source containing the transient settings to be updated. They will not survive a full cluster restart
//     */
//    public ClusterReInitRequestBuilder setTransientSettings(String settings, XContentType xContentType) {
//        request.transientSettings(settings, xContentType);
//        return this;
//    }
//
//    /**
//     * Sets the transient settings to be updated. They will not survive a full cluster restart
//     */
//    public ClusterReInitRequestBuilder setTransientSettings(Map settings) {
//        request.transientSettings(settings);
//        return this;
//    }
//
//    /**
//     * Sets the persistent settings to be updated. They will get applied cross restarts
//     */
//    public ClusterReInitRequestBuilder setPersistentSettings(Settings settings) {
//        request.persistentSettings(settings);
//        return this;
//    }
//
//    /**
//     * Sets the persistent settings to be updated. They will get applied cross restarts
//     */
//    public ClusterReInitRequestBuilder setPersistentSettings(Settings.Builder settings) {
//        request.persistentSettings(settings);
//        return this;
//    }
//
//    /**
//     * Sets the source containing the persistent settings to be updated. They will get applied cross restarts
//     */
//    public ClusterReInitRequestBuilder setPersistentSettings(String settings, XContentType xContentType) {
//        request.persistentSettings(settings, xContentType);
//        return this;
//    }
//
//    /**
//     * Sets the persistent settings to be updated. They will get applied cross restarts
//     */
//    public ClusterReInitRequestBuilder setPersistentSettings(Map settings) {
//        request.persistentSettings(settings);
//        return this;
//    }
}
