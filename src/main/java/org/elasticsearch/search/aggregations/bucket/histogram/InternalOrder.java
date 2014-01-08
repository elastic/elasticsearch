/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.bucket.Bucket;

import java.io.IOException;
import java.util.Comparator;

/**
 * An internal {@link HistogramBase.Order} strategy which is identified by a unique id.
 */
class InternalOrder extends HistogramBase.Order {

    final byte id;
    final String key;
    final boolean asc;
    final Comparator<HistogramBase.Bucket> comparator;

    InternalOrder(byte id, String key, boolean asc, Comparator<HistogramBase.Bucket> comparator) {
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
    Comparator<HistogramBase.Bucket> comparator() {
        return comparator;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field(key, asc ? "asc" : "desc").endObject();
    }

    static class Aggregation extends InternalOrder {

        static final byte ID = 0;

        Aggregation(String key, boolean asc) {
            super(ID, key, asc, new Bucket.SubAggregationComparator<HistogramBase.Bucket>(key, asc));
        }

        Aggregation(String aggName, String valueName, boolean asc) {
            super(ID, key(aggName, valueName), asc, new Bucket.SubAggregationComparator<HistogramBase.Bucket>(aggName, valueName, asc));
        }

        private static String key(String aggName, String valueName) {
            return (valueName == null) ? aggName : aggName + "." + valueName;
        }

    }

    static class Streams {

        /**
         * Writes the given order to the given output (based on the id of the order).
         */
        public static void writeOrder(InternalOrder order, StreamOutput out) throws IOException {
            out.writeByte(order.id());
            if (order instanceof InternalOrder.Aggregation) {
                out.writeBoolean(order.asc());
                out.writeString(order.key());
            }
        }

        /**
         * Reads an order from the given input (based on the id of the order).
         *
         * @see Streams#writeOrder(InternalOrder, org.elasticsearch.common.io.stream.StreamOutput)
         */
        public static InternalOrder readOrder(StreamInput in) throws IOException {
            byte id = in.readByte();
            switch (id) {
                case 1: return (InternalOrder) Histogram.Order.KEY_ASC;
                case 2: return (InternalOrder) Histogram.Order.KEY_DESC;
                case 3: return (InternalOrder) Histogram.Order.COUNT_ASC;
                case 4: return (InternalOrder) Histogram.Order.COUNT_DESC;
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
