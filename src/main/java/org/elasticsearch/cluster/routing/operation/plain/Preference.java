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
public enum  Preference {

    /**
     * Route to specific shards
     */
    SHARDS("_shards"),

    /**
     * Route to preferred node, if possible
     */
    PREFER_NODE("_prefer_node"),

    /**
     * Route to local node, if possible
     */
    LOCAL("_local"),

    /**
     * Route to primary shards
     */
    PRIMARY("_primary"),

    /**
     * Route to primary shards first
     */
    PRIMARY_FIRST("_primary_first"),

    /**
     * Route to the local shard only
     */
    ONLY_LOCAL("_only_local"),

    /**
     * Route to specific node only
     */
    ONLY_NODE("_only_node");

    private final String type;

    private String value = null;

    Preference(String type) {
        this.type = type;
    }

    public String value() {
        return value;
    }

    public String type() {
        return type;
    }

    private Preference setValue(String value) {
        this.value = value;
        return this;
    }

    public static Preference fromString(String preference) {
        String preferenceType;
        String value = null;
        int colonIndex = preference.indexOf(":");
        if (colonIndex == -1) {
            preferenceType = preference;
        } else {
            value = preference.substring(colonIndex+1);
            preferenceType = preference.substring(0, colonIndex);
        }

        switch (preferenceType) {
            case "_shards":
                return SHARDS.setValue(value);
            case "_prefer_node":
                return PREFER_NODE.setValue(value);
            case "_local":
                return LOCAL;
            case "_primary":
                return PRIMARY;
            case "_primary_first":
            case "_primaryFirst":
                return PRIMARY_FIRST;
            case "_only_local":
            case "_onlyLocal":
                return ONLY_LOCAL;
            case "_only_node":
                return ONLY_NODE.setValue(value);
        }
        return null;
    }
}


