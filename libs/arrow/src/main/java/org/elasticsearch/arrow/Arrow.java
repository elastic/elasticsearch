/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.arrow;

import org.apache.arrow.memory.RootAllocator;

public class Arrow {

    /**
     * Arrow IPC stream media type.
     *
     * @see <a href="https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format">Format docs</a>
     * @see <a href="https://www.iana.org/assignments/media-types/application/vnd.apache.arrow.stream">IANA assignment</a>
     */
    public static String MEDIA_TYPE = "application/vnd.apache.arrow.stream";

    private static final RootAllocator ROOT_ALLOCATOR = new RootAllocator();

    /**
     * Returns the global root allocator. Should be used to create child allocators to have
     * fine-grained memory allocation tracking and to enforce local limits.
     */
    public static RootAllocator rootAllocator() {
        return ROOT_ALLOCATOR;
    }
}
