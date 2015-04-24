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

package org.elasticsearch.index.engine;

import com.google.common.collect.Iterators;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class Segment implements Streamable {

    private String name;
    private long generation;
    public boolean committed;
    public boolean search;
    public long sizeInBytes = -1;
    public int docCount = -1;
    public int delDocCount = -1;
    public org.apache.lucene.util.Version version = null;
    public Boolean compound = null;
    public String mergeId;
    public long memoryInBytes;
    public Accountable ramTree = null;

    Segment() {
    }

    public Segment(String name) {
        this.name = name;
        this.generation = Long.parseLong(name.substring(1), Character.MAX_RADIX);
    }

    public String getName() {
        return this.name;
    }

    public long getGeneration() {
        return this.generation;
    }

    public boolean isCommitted() {
        return this.committed;
    }

    public boolean isSearch() {
        return this.search;
    }

    public int getNumDocs() {
        return this.docCount;
    }

    public int getDeletedDocs() {
        return this.delDocCount;
    }

    public ByteSizeValue getSize() {
        return new ByteSizeValue(sizeInBytes);
    }

    public long getSizeInBytes() {
        return this.sizeInBytes;
    }

    public org.apache.lucene.util.Version getVersion() {
        return version;
    }

    @Nullable
    public Boolean isCompound() {
        return compound;
    }

    /**
     * If set, a string representing that the segment is part of a merge, with the value representing the
     * group of segments that represent this merge.
     */
    @Nullable
    public String getMergeId() {
        return this.mergeId;
    }

    /**
     * Estimation of the memory usage used by a segment.
     */
    public long getMemoryInBytes() {
        return this.memoryInBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Segment segment = (Segment) o;

        if (name != null ? !name.equals(segment.name) : segment.name != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    public static Segment readSegment(StreamInput in) throws IOException {
        Segment segment = new Segment();
        segment.readFrom(in);
        return segment;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        generation = Long.parseLong(name.substring(1), Character.MAX_RADIX);
        committed = in.readBoolean();
        search = in.readBoolean();
        docCount = in.readInt();
        delDocCount = in.readInt();
        sizeInBytes = in.readLong();
        version = Lucene.parseVersionLenient(in.readOptionalString(), null);
        compound = in.readOptionalBoolean();
        mergeId = in.readOptionalString();
        memoryInBytes = in.readLong();
        if (in.readBoolean()) {
            // verbose mode
            ramTree = readRamTree(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeBoolean(committed);
        out.writeBoolean(search);
        out.writeInt(docCount);
        out.writeInt(delDocCount);
        out.writeLong(sizeInBytes);
        out.writeOptionalString(version.toString());
        out.writeOptionalBoolean(compound);
        out.writeOptionalString(mergeId);
        out.writeLong(memoryInBytes);
        
        boolean verbose = ramTree != null;
        out.writeBoolean(verbose);
        if (verbose) {
            writeRamTree(out, ramTree);
        }
    }

    Accountable readRamTree(StreamInput in) throws IOException {
        final String name = in.readString();
        final long bytes = in.readVLong();
        int numChildren = in.readVInt();
        if (numChildren == 0) {
            return Accountables.namedAccountable(name, bytes);
        }
        List<Accountable> children = new ArrayList(numChildren);
        while (numChildren-- > 0) {
            children.add(readRamTree(in));
        }
        return Accountables.namedAccountable(name, children, bytes);
    }
    
    // the ram tree is written recursively since the depth is fairly low (5 or 6)
    void writeRamTree(StreamOutput out, Accountable tree) throws IOException {
        out.writeString(tree.toString());
        out.writeVLong(tree.ramBytesUsed());
        Collection<Accountable> children = tree.getChildResources();
        out.writeVInt(children.size());
        for (Accountable child : children) {
            writeRamTree(out, child);
        }
    }

    @Override
    public String toString() {
        return "Segment{" +
                "name='" + name + '\'' +
                ", generation=" + generation +
                ", committed=" + committed +
                ", search=" + search +
                ", sizeInBytes=" + sizeInBytes +
                ", docCount=" + docCount +
                ", delDocCount=" + delDocCount +
                ", version='" + version + '\'' +
                ", compound=" + compound +
                ", mergeId='" + mergeId + '\'' +
                ", memoryInBytes=" + memoryInBytes +
                '}';
    }
}