/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

/**
 * Represents a unit of work for a connector to execute.
 * Connectors that support parallel reads return multiple splits from
 * {@link Connector#discoverSplits}; simple connectors use {@link #SINGLE}.
 */
public interface Split {
    Split SINGLE = new Split() {
        @Override
        public String toString() {
            return "Split[SINGLE]";
        }
    };
}
