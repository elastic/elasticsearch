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

package org.elasticsearch.index.shard;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.engine.Engine;

import java.io.Closeable;
import java.io.IOException;

/**
 * Used in {@link IndexShard} to hold the reference to the current {@link Engine}.
 */
final class EngineReference implements Closeable {
    private boolean closed;
    private volatile Engine current;

    Engine get() {
        return current;
    }

    /**
     * Closes the current engine and replaces with the new engine. If this method succeeds, the ownership of the new engine
     * is transferred to this reference. Thus, we must not call close directly on the new engine. If this reference was closed,
     * this method will throw {@link AlreadyClosedException} and the caller need to release the new engine.
     *
     * @throws AlreadyClosedException if this holder was closed already
     * @throws IOException            if fail to close the current engine
     */
    void swapReference(Engine newEngine) throws IOException, AlreadyClosedException {
        final Engine toClose;
        synchronized (this) {
            if (closed) {
                assert current == null;
                throw new AlreadyClosedException("engine reference was closed");
            }
            toClose = current;
            current = newEngine;
        }
        IOUtils.close(toClose);
    }

    void flushAndClose() throws IOException {
        doClose(true);
    }

    @Override
    public void close() throws IOException {
        doClose(false);
    }

    private void doClose(boolean flush) throws IOException {
        final Engine toClose;
        synchronized (this) {
            assert closed == false || current == null;
            toClose = current;
            current = null;
            closed = true;
        }
        if (toClose != null && flush) {
            toClose.flushAndClose();
        } else {
            IOUtils.close(toClose);
        }
    }
}
