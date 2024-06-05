/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

/**
 * A wrapping processor is one that encapsulates an inner processor, or a processor that the wrapped processor acts upon. All processors
 * that contain an "inner" processor should implement this interface, such that the actual processor can be obtained.
 */
public interface WrappingProcessor extends Processor {

    /**
     * Method for retrieving the inner processor from a wrapped processor.
     * @return the inner processor
     */
    Processor getInnerProcessor();

    default boolean isAsync() {
        return getInnerProcessor().isAsync();
    }
}
