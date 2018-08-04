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

import org.elasticsearch.common.transport.TransportAddress;

import java.util.List;
import java.util.function.Consumer;

public interface ConfiguredHostsResolver {
    /**
     * Attempt to resolve the configured unicast hosts list to a list of transport addresses.
     *
     * @param consumer Consumer for the resolved list. May not be called if an error occurs or if another resolution attempt is in
     *                 progress.
     */
    void resolveConfiguredHosts(Consumer<List<TransportAddress>> consumer);
}

