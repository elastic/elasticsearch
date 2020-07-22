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
package org.elasticsearch.cluster;

/**
 * Enables listening to master changes events of the local node (when the local node becomes the master, and when the local
 * node cease being a master).
 */
public interface LocalNodeMasterListener {

    /**
     * Called when local node is elected to be the master
     */
    void onMaster();

    /**
     * Called when the local node used to be the master, a new master was elected and it's no longer the local node.
     */
    void offMaster();
}

