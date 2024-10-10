/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public interface DownsampleFieldSerializer {

    /**
     * Serialize the downsampled value of the field.
     */
    void write(XContentBuilder builder) throws IOException;
}
