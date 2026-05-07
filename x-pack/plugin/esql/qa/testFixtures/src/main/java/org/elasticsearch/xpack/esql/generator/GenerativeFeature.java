/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator;

/**
 * Optional behaviors that a generative test run can opt into via {@link GenerationContext}.
 * Features are off by default in the main generative suite so that failures specific to one
 * feature can be isolated to a dedicated test class without muting the rest.
 */
public enum GenerativeFeature {
    /**
     * Allow {@code FROM (...)} subqueries to be produced as sources. See
     * {@code GenerativeSubqueryRestTest}.
     */
    SUBQUERIES
}
