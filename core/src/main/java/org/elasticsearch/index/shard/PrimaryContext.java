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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.seqno.SeqNoPrimaryContext;

import java.io.IOException;

/**
 * Represents the primary context which encapsulates the view the primary shard has of its replication group.
 */
public class PrimaryContext implements Writeable {

    private final SeqNoPrimaryContext seqNoPrimaryContext;

    public SeqNoPrimaryContext seqNoPrimaryContext() {
        return seqNoPrimaryContext;
    }

    /**
     * Construct a primary context with the sequence number primary context.
     *
     * @param seqNoPrimaryContext the sequence number primary context
     */
    public PrimaryContext(final SeqNoPrimaryContext seqNoPrimaryContext) {
        this.seqNoPrimaryContext = seqNoPrimaryContext;
    }

    /**
     * Construct a primary context from a stream.
     *
     * @param in the stream
     * @throws IOException if an I/O exception occurs reading from the stream
     */
    public PrimaryContext(final StreamInput in) throws IOException {
        seqNoPrimaryContext = new SeqNoPrimaryContext(in);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        seqNoPrimaryContext.writeTo(out);
    }

    @Override
    public String toString() {
        return "PrimaryContext{" +
                "seqNoPrimaryContext=" + seqNoPrimaryContext +
                '}';
    }
}
