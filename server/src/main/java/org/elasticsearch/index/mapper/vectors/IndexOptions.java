/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;

/**
 * Represents general index options that can be attached to a semantic or vector field.
 */
public abstract class IndexOptions implements ToXContent {

    public IndexOptions() {}

    public abstract IndexOptions readFrom(StreamInput in) throws IOException;

    public static IndexOptions readIndexOptions(StreamInput in) throws IOException {
        String className = in.readString(); // Read the class name from the input
        try {
            Class<?> clazz = Class.forName(className);
            IndexOptions instance = (IndexOptions) clazz.getConstructor().newInstance();
            return instance.readFrom(in);
        } catch (ReflectiveOperationException e) {
            throw new IOException("Failed to create instance of " + className, e);
        }
    }
}
