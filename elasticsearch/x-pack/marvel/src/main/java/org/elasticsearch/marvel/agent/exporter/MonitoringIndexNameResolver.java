/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter;

/**
 *
 */
public interface MonitoringIndexNameResolver {

    String resolve(MarvelDoc doc);

    String resolve(long timestamp);

    /**
     * Returns the generic part of the index name (ie without any dynamic part like a timestamp) that can be used to match indices names.
     *
     * @return the index pattern
     */
    String indexPattern();

}
