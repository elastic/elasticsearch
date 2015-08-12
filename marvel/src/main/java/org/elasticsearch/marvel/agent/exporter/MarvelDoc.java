/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter;

public abstract class MarvelDoc<T> {

    private final String clusterName;
    private final String type;
    private final long timestamp;

    public MarvelDoc(String clusterName, String type, long timestamp) {
        this.clusterName = clusterName;
        this.type = type;
        this.timestamp = timestamp;
    }

    public String clusterName() {
        return clusterName;
    }

    public String type() {
        return type;
    }

    public long timestamp() {
        return timestamp;
    }

    public abstract T payload();
}