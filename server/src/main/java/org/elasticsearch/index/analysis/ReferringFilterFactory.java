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

package org.elasticsearch.index.analysis;

import java.util.Map;

/**
 * Marks a {@link TokenFilterFactory} that refers to other filter factories.
 *
 * The analysis registry will call {@link #setReferences(Map)} with a map of all
 * available TokenFilterFactories after all factories have been registered
 */
public interface ReferringFilterFactory {

    /**
     * Called with a map of all registered filter factories
     */
    void setReferences(Map<String, TokenFilterFactory> factories);

}
