/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore.stream.write;

import org.elasticsearch.common.StreamContext;

@FunctionalInterface
public interface StreamContextSupplier {

    /**
     * @param partSize The size of a single part to be uploaded
     * @return The <code>StreamContext</code> based on the part size provided
     */
    StreamContext supplyStreamContext(long partSize);
}
