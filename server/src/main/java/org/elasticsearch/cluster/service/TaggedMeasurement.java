/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.service;

public final class TaggedMeasurement {
    private final Double value;
    private final Tags tags;

    /**
     * Factory method to create the {@link TaggedMeasurement} object.
     * @param value value.
     * @param tags tags to be added per value.
     * @return tagged measurement TaggedMeasurement
     */
    public static TaggedMeasurement create(double value, Tags tags) {
        return new TaggedMeasurement(value, tags);
    }

    private TaggedMeasurement(double value, Tags tags) {
        this.value = value;
        this.tags = tags;
    }

    /**
     * Returns the value.
     * @return value
     */
    public Double getValue() {
        return value;
    }

    /**
     * Returns the tags.
     * @return tags
     */
    public Tags getTags() {
        return tags;
    }
}
