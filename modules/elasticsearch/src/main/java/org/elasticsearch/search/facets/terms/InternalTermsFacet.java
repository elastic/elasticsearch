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

package org.elasticsearch.search.facets.terms;

import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.thread.ThreadLocals;
import org.elasticsearch.common.trove.TObjectIntHashMap;
import org.elasticsearch.common.trove.TObjectIntIterator;
import org.elasticsearch.common.xcontent.builder.XContentBuilder;
import org.elasticsearch.search.facets.Facet;
import org.elasticsearch.search.facets.internal.InternalFacet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * @author kimchy (shay.banon)
 */
public class InternalTermsFacet implements InternalFacet, TermsFacet {

    private String name;

    private String fieldName;

    private int requiredSize;

    private Collection<Entry> entries = ImmutableList.of();

    private ComparatorType comparatorType;

    private InternalTermsFacet() {
    }

    public InternalTermsFacet(String name, String fieldName, ComparatorType comparatorType, int requiredSize, Collection<Entry> entries) {
        this.name = name;
        this.fieldName = fieldName;
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

    @Override public String fieldName() {
        return this.fieldName;
    }

    @Override public String getFieldName() {
        return fieldName();
    }

    @Override public Type type() {
        return Type.TERMS;
    }

    @Override public Type getType() {
        return type();
    }

    @Override public List<Entry> entries() {
        if (!(entries instanceof List)) {
            entries = ImmutableList.copyOf(entries);
        }
        return (List<Entry>) entries;
    }

    @Override public List<Entry> getEntries() {
        return entries();
    }

    @Override public Iterator<Entry> iterator() {
        return entries.iterator();
    }

    private static ThreadLocal<ThreadLocals.CleanableValue<TObjectIntHashMap<String>>> aggregateCache = new ThreadLocal<ThreadLocals.CleanableValue<TObjectIntHashMap<String>>>() {
        @Override protected ThreadLocals.CleanableValue<TObjectIntHashMap<String>> initialValue() {
            return new ThreadLocals.CleanableValue<TObjectIntHashMap<String>>(new TObjectIntHashMap<String>());
        }
    };

    @Override public Facet aggregate(Iterable<Facet> facets) {
        TObjectIntHashMap<String> aggregated = aggregateCache.get().get();
        aggregated.clear();

        for (Facet facet : facets) {
            if (!facet.name().equals(name)) {
                continue;
            }
            TermsFacet mFacet = (TermsFacet) facet;
            for (Entry entry : mFacet) {
                aggregated.adjustOrPutValue(entry.term(), entry.count(), entry.count());
            }
        }

        BoundedTreeSet<Entry> ordered = new BoundedTreeSet<Entry>(comparatorType.comparator(), requiredSize);
        for (TObjectIntIterator<String> it = aggregated.iterator(); it.hasNext();) {
            it.advance();
            ordered.add(new Entry(it.key(), it.value()));
        }

        return new InternalTermsFacet(name, fieldName, comparatorType, requiredSize, ordered);
    }

    @Override public void toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field("_type", "terms");
        builder.field("_field", fieldName);
        builder.startArray("terms");
        for (Entry entry : entries) {
            builder.startObject();
            builder.field("term", entry.term());
            builder.field("count", entry.count());
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
    }

    public static InternalTermsFacet readTermsFacet(StreamInput in) throws IOException {
        InternalTermsFacet facet = new InternalTermsFacet();
        facet.readFrom(in);
        return facet;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        name = in.readUTF();
        fieldName = in.readUTF();
        comparatorType = ComparatorType.fromId(in.readByte());
        requiredSize = in.readVInt();

        int size = in.readVInt();
        entries = new ArrayList<Entry>(size);
        for (int i = 0; i < size; i++) {
            entries.add(new Entry(in.readUTF(), in.readVInt()));
        }
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(fieldName);
        out.writeByte(comparatorType.id());

        out.writeVInt(requiredSize);

        out.writeVInt(entries.size());
        for (Entry entry : entries) {
            out.writeUTF(entry.term());
            out.writeVInt(entry.count());
        }
    }
}