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
package org.elasticsearch.cluster.routing.operation.plain;

/**
 * Routing Preference Type
 */
public final class PreferenceType {

    /**
     * Route to specific shards
     */
    public final static String SHARDS = "_shards:";

    /**
     * Route to preferred node, if possible
     */
    public final static String PREFER_NODE = "_prefer_node:";

    /**
     * Route to local node, if possible
     */
    public final static String LOCAL = "_local";

    /**
     * Route to primary shards
     */
    public final static String PRIMARY = "_primary";

    /**
     * Route to primary shards first
     */
    public final static String PRIMARY_FIRST = "_primary_first";

    /**
     * Route to the local shard only
     */
    public final static String ONLY_LOCAL = "_only_local";

    /**
     * Route to specific node only
     */
    public final static String ONLY_NODE = "_only_node:";
}

