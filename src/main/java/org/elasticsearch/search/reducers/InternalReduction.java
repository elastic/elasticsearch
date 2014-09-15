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
package org.elasticsearch.search.reducers;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * An internal implementation of {@link Reduction}. Serves as a base class for all reduction implementations.
 */
public abstract class InternalReduction implements Reduction, ToXContent, Streamable {

    /**
     * The aggregation type that holds all the string types that are associated with an reduction:
     * <ul>
     *     <li>name - used as the parser type</li>
     *     <li>stream - used as the stream type</li>
     * </ul>
     */
    public static class Type {

        private String name;
        private BytesReference stream;

        public Type(String name) {
            this(name, new BytesArray(name));
        }

        public Type(String name, String stream) {
            this(name, new BytesArray(stream));
        }

        public Type(String name, BytesReference stream) {
            this.name = name;
            this.stream = stream;
        }

        /**
         * @return The name of the type (mainly used for registering the parser for the reducer (see {@link org.elasticsearch.search.reducers.Reducer.Parser#type()}).
         */
        public String name() {
            return name;
        }

        /**
         * @return  The name of the stream type (used for registering the reduction stream
         *          (see {@link ReductionStreams#registerStream(ReductionStreams.Stream, org.elasticsearch.common.bytes.BytesReference...)}).
         */
        public BytesReference stream() {
            return stream;
        }

        @Override
        public String toString() {
            return name;
        }
    }


    protected String name;

    /** Constructs an un-initialized addReduction (used for serialization) **/
    protected InternalReduction() {}

    /**
     * Constructs a reduction with a given name.
     *
     * @param name The name of the reduction.
     */
    protected InternalReduction(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    /**
     * @return The {@link Type} of this reduction
     */
    public abstract Type type();

    /**
     * Read a size under the assumption that a value of 0 means unlimited.
     */
    protected static int readSize(StreamInput in) throws IOException {
        final int size = in.readVInt();
        return size == 0 ? Integer.MAX_VALUE : size;
    }

    /**
     * Write a size under the assumption that a value of 0 means unlimited.
     */
    protected static void writeSize(int size, StreamOutput out) throws IOException {
        if (size == Integer.MAX_VALUE) {
            size = 0;
        }
        out.writeVInt(size);
    }
    
    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        doXContentBody(builder, params);
        builder.endObject();
        return builder;
    }

    public abstract XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException;


}
