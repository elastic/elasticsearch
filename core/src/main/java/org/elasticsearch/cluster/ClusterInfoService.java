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
 * Interface for a class used to gather information about a cluster at
 * regular intervals
 */
public interface ClusterInfoService {

    /** The latest cluster information */
    public ClusterInfo getClusterInfo();

    /** Add a listener that will be called every time new information is gathered */
    public void addListener(Listener listener);

    /**
     * Interface for listeners to implement in order to perform actions when
     * new information about the cluster has been gathered
     */
    public interface Listener {
        public void onNewInfo(ClusterInfo info);
    }
}
