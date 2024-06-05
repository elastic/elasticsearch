/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.common.io.stream.Writeable;

/**
 * Represents difference between states of cluster state parts
 */
public interface Diff<T> extends Writeable {

    /**
     * Applies difference to the specified part and returns the resulted part
     */
    T apply(T part);
}
