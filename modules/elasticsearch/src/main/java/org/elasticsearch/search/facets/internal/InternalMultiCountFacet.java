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

package org.elasticsearch.search.facets.internal;

import org.elasticsearch.search.facets.Facet;
import org.elasticsearch.search.facets.MultiCountFacet;
import org.elasticsearch.util.BoundedTreeSet;
import org.elasticsearch.util.ThreadLocals;
import org.elasticsearch.util.collect.ImmutableList;
import org.elasticsearch.util.collect.Lists;
import org.elasticsearch.util.gnu.trove.TObjectIntHashMap;
import org.elasticsearch.util.gnu.trove.TObjectIntIterator;
import org.elasticsearch.util.io.stream.StreamInput;
import org.elasticsearch.util.io.stream.StreamOutput;
import org.elasticsearch.util.xcontent.builder.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * @author kimchy (Shay Banon)
 */
public class InternalMultiCountFacet<T extends Comparable> implements InternalFacet, MultiCountFacet<T> {

    private String name;

    private int requiredSize;

    private Collection<Entry<T>> entries = ImmutableList.of();

    private ValueType valueType;

    private ComparatorType comparatorType;

    private InternalMultiCountFacet() {
    }

    public InternalMultiCountFacet(String name, ValueType valueType, ComparatorType comparatorType, int requiredSize, Collection<Entry<T>> entries) {
        this.name = name;
        this.valueType = valueType;
        this.comparatorType = comparatorType;
        this.requiredSize = requiredSize;
        this.entries = entries;
    }

    @Override public String name() {
        return this.name;
    }

    @Override public String getName() {
        return this.name;
    }

    @Override public Type type() {
        return Type.MULTI_COUNT;
    }

    @Override public Type getType() {
        return type();
    }

    public ValueType valueType() {
        return valueType;
    }

    public ValueType getValueType() {
        return valueType();
    }

    @Override public List<Entry<T>> entries() {
        return Lists.newArrayList(this);
    }

    @Override public List<Entry<T>> getEntries() {
        return Lists.newArrayList(this);
    }

    @Override public Iterator<Entry<T>> iterator() {
        return entries.iterator();
    }

    private static ThreadLocal<ThreadLocals.CleanableValue<TObjectIntHashMap<Object>>> aggregateCache = new ThreadLocal<ThreadLocals.CleanableValue<TObjectIntHashMap<Object>>>() {
        @Override protected ThreadLocals.CleanableValue<TObjectIntHashMap<Object>> initialValue() {
            return new ThreadLocals.CleanableValue<TObjectIntHashMap<java.lang.Object>>(new TObjectIntHashMap<Object>());
        }
    };

    @Override public Facet aggregate(Iterable<Facet> facets) {
        TObjectIntHashMap<Object> aggregated = aggregateCache.get().get();
        aggregated.clear();

        for (Facet facet : facets) {
            if (!facet.name().equals(name)) {
                continue;
            }
            MultiCountFacet<T> mFacet = (MultiCountFacet<T>) facet;
            for (Entry<T> entry : mFacet) {
                aggregated.adjustOrPutValue(entry.value(), entry.count(), entry.count());
            }
        }

        BoundedTreeSet<Entry<T>> ordered = new BoundedTreeSet<Entry<T>>(comparatorType.comparator(), requiredSize);
        for (TObjectIntIterator<Object> it = aggregated.iterator(); it.hasNext();) {
            it.advance();
            ordered.add(new Entry<T>((T) it.key(), it.value()));
        }

        return new InternalMultiCountFacet<T>(name, valueType, comparatorType, requiredSize, ordered);
    }

    @Override public void toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(name());
        for (Entry<T> entry : entries) {
            builder.startObject();
            builder.field("value", entry.value());
            builder.field("count", entry.count());
            builder.endObject();
        }
        builder.endArray();
    }

    public static InternalMultiCountFacet readMultiCountFacet(StreamInput in) throws IOException {
        InternalMultiCountFacet facet = new InternalMultiCountFacet();
        facet.readFrom(in);
        return facet;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        name = in.readUTF();
        valueType = ValueType.fromId(in.readByte());
        comparatorType = ComparatorType.fromId(in.readByte());
        requiredSize = in.readVInt();

        int size = in.readVInt();
        entries = new ArrayList<Entry<T>>(size);
        for (int i = 0; i < size; i++) {
            Object value = null;
            if (valueType == ValueType.STRING) {
                value = in.readUTF();
            } else if (valueType == ValueType.SHORT) {
                value = in.readShort();
            } else if (valueType == ValueType.INT) {
                value = in.readInt();
            } else if (valueType == ValueType.LONG) {
                value = in.readLong();
            } else if (valueType == ValueType.FLOAT) {
                value = in.readFloat();
            } else if (valueType == ValueType.DOUBLE) {
                value = in.readDouble();
            }

            entries.add(new Entry<T>((T) value, in.readVInt()));
        }
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeUTF(name);
        out.writeByte(valueType.id());
        out.writeByte(comparatorType.id());

        out.writeVInt(requiredSize);

        out.writeVInt(entries.size());
        for (Entry<T> entry : entries) {
            if (valueType == ValueType.STRING) {
                out.writeUTF((String) entry.value());
            } else if (valueType == ValueType.SHORT) {
                out.writeShort((Short) entry.value());
            } else if (valueType == ValueType.INT) {
                out.writeInt((Integer) entry.value());
            } else if (valueType == ValueType.LONG) {
                out.writeLong((Long) entry.value());
            } else if (valueType == ValueType.FLOAT) {
                out.writeFloat((Float) entry.value());
            } else if (valueType == ValueType.DOUBLE) {
                out.writeDouble((Double) entry.value());
            }
            out.writeVInt(entry.count());
        }
    }
}
