/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

public interface Derivative extends SimpleValue {

    /**
     * Returns the normalized value. If no normalised factor has been specified
     * this method will return {@link #value()}
     *
     * @return the normalized value
     */
    double normalizedValue();
}
