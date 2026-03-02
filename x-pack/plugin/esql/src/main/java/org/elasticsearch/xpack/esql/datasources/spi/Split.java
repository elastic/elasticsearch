/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

/**
 * Represents a unit of work for a connector to execute.
 * Simple connectors use {@link #SINGLE}; connectors that support parallel
 * reads receive {@link ExternalSplit} instances instead.
 */
public interface Split {
    Split SINGLE = new Split() {
        @Override
        public String toString() {
            return "Split[SINGLE]";
        }
    };
}
