/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.common.settings.Settings;

/**
 * An {@code IndexCreationListener} is a callback called prior to the final creation of a new
 * index's settings
 */
public interface IndexCreationListener {
    /**
     * Method called prior to the index settings being created. The settings may be changed using
     * the builder if desired. This handler is only called for new indexes if they are *not*
     * inheriting an existing index's metadata (for example, indices created from a shrink request)
     */
    default void beforeIndexCreated(String indexName, Settings.Builder indexSettings) { }
}
