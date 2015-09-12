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

package org.elasticsearch.discovery;

/**
 * A listener that should be called by the {@link org.elasticsearch.discovery.Discovery} component
 * when the first valid initial cluster state has been submitted and processed by the cluster service.
 * <p/>
 * <p>Note, this listener should be registered with the discovery service before it has started.
 *
 *
 */
public interface InitialStateDiscoveryListener {

    void initialStateProcessed();
}
