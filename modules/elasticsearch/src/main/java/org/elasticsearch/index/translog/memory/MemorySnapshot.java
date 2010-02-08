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

import com.google.common.collect.Iterables;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.index.translog.Translog;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import static org.elasticsearch.index.translog.TranslogStreams.*;

/**
 * @author kimchy (Shay Banon)
 */
public class MemorySnapshot implements Translog.Snapshot {

    private long id;

    Translog.Operation[] operations;

    public MemorySnapshot() {
    }

    public MemorySnapshot(Translog.Snapshot snapshot) {
        this(snapshot.translogId(), Iterables.toArray(snapshot, Translog.Operation.class));
    }

    public MemorySnapshot(long id, Translog.Operation[] operations) {
        this.id = id;
        this.operations = operations;
    }

    @Override public long translogId() {
        return id;
    }

    @Override public boolean release() throws ElasticSearchException {
        return true;
    }

    @Override public int size() {
        return operations.length;
    }

    @Override public Iterator<Translog.Operation> iterator() {
        return Arrays.asList(operations).iterator();
    }

    @Override public Iterable<Translog.Operation> skipTo(int skipTo) {
        if (operations.length < skipTo) {
            throw new ElasticSearchIllegalArgumentException("skipTo [" + skipTo + "] is bigger than size [" + size() + "]");
        }
        return Arrays.asList(Arrays.copyOfRange(operations, skipTo, operations.length));
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        id = in.readLong();
        operations = new Translog.Operation[in.readInt()];
        for (int i = 0; i < operations.length; i++) {
            operations[i] = readTranslogOperation(in);
        }
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        out.writeLong(id);
        out.writeInt(operations.length);
        for (Translog.Operation op : operations) {
            writeTranslogOperation(out, op);
        }
    }
}
