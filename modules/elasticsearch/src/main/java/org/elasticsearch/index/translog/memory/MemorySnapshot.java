/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.translog.memory;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.index.translog.Translog;

import java.util.Iterator;
import java.util.Queue;

/**
 * @author kimchy (shay.banon)
 */
public class MemorySnapshot implements Translog.Snapshot {

    private final long id;

    private final Iterator<Translog.Operation> operationsIt;

    private final long length;

    private long position = 0;

    public MemorySnapshot(long id, Queue<Translog.Operation> operations, long length) {
        this.id = id;
        this.operationsIt = operations.iterator();
        this.length = length;
    }

    @Override public long translogId() {
        return id;
    }

    @Override public boolean release() throws ElasticSearchException {
        return true;
    }

    @Override public long length() {
        return length;
    }

    @Override public long position() {
        return this.position;
    }

    @Override public boolean hasNext() {
        return position < length;
    }

    @Override public Translog.Operation next() {
        Translog.Operation operation = operationsIt.next();
        position++;
        return operation;
    }

    @Override public void seekForward(long length) {
        long numberToSeek = this.position + length;
        while (numberToSeek-- != 0) {
            operationsIt.next();
        }
        this.position += length;
    }
}
