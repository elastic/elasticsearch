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

package org.elasticsearch.client.core;


import java.util.Locale;

/**
 * IndexerState represents the internal state of the indexer.  It
 * is also persistent when changing from started/stopped in case the allocated
 * task is restarted elsewhere.
 */
public enum IndexerState {
    /** Indexer is running, but not actively indexing data (e.g. it's idle). */
    STARTED,

    /** Indexer is actively indexing data. */
    INDEXING,

    /**
     * Transition state to where an indexer has acknowledged the stop
     * but is still in process of halting.
     */
    STOPPING,

    /** Indexer is "paused" and ignoring scheduled triggers. */
    STOPPED,

    /**
     * Something (internal or external) has requested the indexer abort
     * and shutdown.
     */
    ABORTING;

    public static IndexerState fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public String value() {
        return name().toLowerCase(Locale.ROOT);
    }
}