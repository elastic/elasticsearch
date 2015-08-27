/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter;

public abstract class MarvelDoc {

    private final String index;
    private final String type;
    private final String id;

    private final String clusterUUID;
    private final long timestamp;

    public MarvelDoc(String index, String type, String id, String clusterUUID, long timestamp) {
        this.index = index;
        this.type = type;
        this.id = id;
        this.clusterUUID = clusterUUID;
        this.timestamp = timestamp;
    }

    public MarvelDoc(String clusterUUID, String type, long timestamp) {
        this(null, type, null, clusterUUID, timestamp);
    }

    public String clusterUUID() {
        return clusterUUID;
    }

    public String index() {
        return index;
    }

    public String type() {
        return type;
    }

    public String id() {
        return id;
    }

    public long timestamp() {
        return timestamp;
    }
}