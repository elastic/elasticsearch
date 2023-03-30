/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2021 Elasticsearch B.V.
 */
package org.elasticsearch.xpack.lucene.bwc.codecs.lucene70.fst;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.Accountable;

import java.io.IOException;

/**
 * Represents the outputs for an FST, providing the basic algebra required for building and
 * traversing the FST.
 *
 * <p>Note that any operation that returns NO_OUTPUT must return the same singleton object from
 * {@link #getNoOutput}.
 *
 */
public abstract class Outputs<T> {

    // TODO: maybe change this API to allow for re-use of the
    // output instances -- this is an insane amount of garbage
    // (new object per byte/char/int) if eg used during
    // analysis

    /** Eg common("foobar", "food") -&gt; "foo" */
    public abstract T common(T output1, T output2);

    /** Eg subtract("foobar", "foo") -&gt; "bar" */
    public abstract T subtract(T output, T inc);

    /** Eg add("foo", "bar") -&gt; "foobar" */
    public abstract T add(T prefix, T output);

    /** Encode an output value into a {@link DataOutput}. */
    public abstract void write(T output, DataOutput out) throws IOException;

    /**
     * Encode an final node output value into a {@link DataOutput}. By default this just calls {@link
     * #write(Object, DataOutput)}.
     */
    public void writeFinalOutput(T output, DataOutput out) throws IOException {
        write(output, out);
    }

    /** Decode an output value previously written with {@link #write(Object, DataOutput)}. */
    public abstract T read(DataInput in) throws IOException;

    /** Skip the output; defaults to just calling {@link #read} and discarding the result. */
    public void skipOutput(DataInput in) throws IOException {
        read(in);
    }

    /**
     * Decode an output value previously written with {@link #writeFinalOutput(Object, DataOutput)}.
     * By default this just calls {@link #read(DataInput)}.
     */
    public T readFinalOutput(DataInput in) throws IOException {
        return read(in);
    }

    /**
     * Skip the output previously written with {@link #writeFinalOutput}; defaults to just calling
     * {@link #readFinalOutput} and discarding the result.
     */
    public void skipFinalOutput(DataInput in) throws IOException {
        skipOutput(in);
    }

    /**
     * NOTE: this output is compared with == so you must ensure that all methods return the single
     * object if it's really no output
     */
    public abstract T getNoOutput();

    public abstract String outputToString(T output);

    // TODO: maybe make valid(T output) public...? for asserts

    public T merge(T first, T second) {
        throw new UnsupportedOperationException();
    }

    /**
     * Return memory usage for the provided output.
     *
     * @see Accountable
     */
    public abstract long ramBytesUsed(T output);
}
