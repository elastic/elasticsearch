/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter;

public abstract class MarvelDoc<T> {

    private final String index;
    private final String type;
    private final String id;

    private final String clusterName;
    private final long timestamp;

    public MarvelDoc(String index, String type, String id, String clusterName, long timestamp) {
        this.index = index;
        this.type = type;
        this.id = id;
        this.clusterName = clusterName;
        this.timestamp = timestamp;
    }

    public MarvelDoc(String clusterName, String type, long timestamp) {
        this(null, type, null, clusterName, timestamp);
    }

    public String clusterName() {
        return clusterName;
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

    public abstract T payload();
}