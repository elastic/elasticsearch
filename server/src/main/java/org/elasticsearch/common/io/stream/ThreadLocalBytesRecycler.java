/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.transport.BytesRefRecycler;

public class ThreadLocalBytesRecycler {

    public static final ThreadLocalBytesRecycler NON_RECYCLING_INSTANCE = new ThreadLocalBytesRecycler(
        BytesRefRecycler.NON_RECYCLING_INSTANCE
    );

    private final ThreadLocal<RecyclerBytesStreamOutput> bytesRecyclerStream;

    public ThreadLocalBytesRecycler(BytesRefRecycler bytesRefRecycler) {
        this.bytesRecyclerStream = ThreadLocal.withInitial(() -> new RecyclerBytesStreamOutput(bytesRefRecycler));
    }

    public RecyclerBytesStreamOutput get() {
        return bytesRecyclerStream.get();
    }
}
