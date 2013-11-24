/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Comparator;

/**
 *
 */
class InternalOrder extends Terms.Order {

    final byte id;
    final String key;
    final boolean asc;
    final Comparator<Terms.Bucket> comparator;

    InternalOrder(byte id, String key, boolean asc, Comparator<Terms.Bucket> comparator) {
        this.id = id;
        this.key = key;
        this.asc = asc;
        this.comparator = comparator;
    }

    byte id() {
        return id;
    }

    String key() {
        return key;
    }

    boolean asc() {
        return asc;
    }

    @Override
    protected Comparator<Terms.Bucket> comparator() {
        return comparator;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field(key, asc ? "asc" : "desc").endObject();
    }

    static class Aggregation extends InternalOrder {

        static final byte ID = 0;

        Aggregation(String key, boolean asc) {
            super(ID, key, asc, new Terms.Bucket.Comparator<Terms.Bucket>(key, asc));
        }

        Aggregation(String aggName, String valueName, boolean asc) {
            super(ID, key(aggName, valueName), asc, new Terms.Bucket.Comparator<Terms.Bucket>(aggName, valueName, asc));
        }

        private static String key(String aggName, String valueName) {
            return (valueName == null) ? aggName : aggName + "." + valueName;
        }

    }

    public static class Streams {

        public static void writeOrder(InternalOrder order, StreamOutput out) throws IOException {
            out.writeByte(order.id());
            if (order instanceof Aggregation) {
                out.writeBoolean(((Terms.Bucket.Comparator) order.comparator).asc());
                out.writeString(((Terms.Bucket.Comparator) order.comparator).aggName());
                boolean hasValueName = ((Terms.Bucket.Comparator) order.comparator).aggName() != null;
                out.writeBoolean(hasValueName);
                if (hasValueName) {
                    out.writeString(((Terms.Bucket.Comparator) order.comparator).valueName());
                }
            }
        }

        public static InternalOrder readOrder(StreamInput in) throws IOException {
            byte id = in.readByte();
            switch (id) {
                case 1: return (InternalOrder) Terms.Order.COUNT_DESC;
                case 2: return (InternalOrder) Terms.Order.COUNT_ASC;
                case 3: return (InternalOrder) Terms.Order.TERM_DESC;
                case 4: return (InternalOrder) Terms.Order.TERM_ASC;
                case 0:
                    boolean asc = in.readBoolean();
                    String key = in.readString();
                    return new InternalOrder.Aggregation(key, asc);
                default:
                    throw new RuntimeException("unknown histogram order");
            }
        }
    }
}
