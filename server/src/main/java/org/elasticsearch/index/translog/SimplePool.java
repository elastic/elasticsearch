/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.translog;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;

import java.util.concurrent.ConcurrentLinkedQueue;

public class SimplePool implements Recycler<BytesRef> {

    private final ConcurrentLinkedQueue<BytesRef> queue = new ConcurrentLinkedQueue<>();

    public SimplePool(Settings settings) {
        int size = EsExecutors.allocatedProcessors(settings);
        for (int i = 0; i < size; i++) {
            queue.add(new BytesRef(new byte[256]));
        }
    }

    @Override
    public V<BytesRef> obtain() {
        BytesRef ref = queue.poll();
        final BytesRef finalRef = ref == null ? new BytesRef(new byte[256]) : ref;
        final boolean pooled = ref == finalRef;
        return new V<>() {
            @Override
            public BytesRef v() {
                return finalRef;
            }

            @Override
            public boolean isRecycled() {
                return pooled;
            }

            @Override
            public void close() {
                if (pooled) {
                    queue.add(finalRef);
                }
            }
        };
    }

    @Override
    public int pageSize() {
        return 256;
    }
}
