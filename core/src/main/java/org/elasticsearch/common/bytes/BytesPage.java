/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.bytes;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lease.Releasable;

import java.util.concurrent.atomic.AtomicBoolean;

public class BytesPage extends BytesArray implements Releasable {

    private final Releasable recycler;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    public BytesPage(byte[] page, Releasable recycler) {
        super(page);
        this.recycler = recycler;
    }

    public BytesPage(BytesRef pageRef) {
        super(pageRef);
        this.recycler = null;
    }

    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            if (recycler != null) {
                recycler.close();
            }
        } else {
            throw new IllegalStateException("Attempting to close BytesPage that is already closed.");
        }
    }
}
