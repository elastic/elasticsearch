/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.index.engine;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.IOException;

public class Segment implements Streamable {

    private String name;
    private long generation;
    public boolean committed;
    public boolean search;
    public long sizeInBytes = -1;
    public int docCount = -1;
    public int delDocCount = -1;

    Segment() {
    }

    public Segment(String name) {
        this.name = name;
        this.generation = Long.parseLong(name.substring(1), Character.MAX_RADIX);
    }

    public String name() {
        return this.name;
    }

    public String getName() {
        return name();
    }

    public long generation() {
        return this.generation;
    }

    public long getGeneration() {
        return this.generation;
    }

    public boolean committed() {
        return this.committed;
    }

    public boolean isCommitted() {
        return this.committed;
    }

    public boolean search() {
        return this.search;
    }

    public boolean isSearch() {
        return this.search;
    }

    public int numDocs() {
        return this.docCount;
    }

    public int getNumDocs() {
        return this.docCount;
    }

    public int deletedDocs() {
        return this.delDocCount;
    }

    public int getDeletedDocs() {
        return this.delDocCount;
    }

    public ByteSizeValue size() {
        return new ByteSizeValue(sizeInBytes);
    }

    public ByteSizeValue getSize() {
        return size();
    }

    public long sizeInBytes() {
        return sizeInBytes;
    }

    public long getSizeInBytes() {
        return sizeInBytes();
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
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeBoolean(committed);
        out.writeBoolean(search);
        out.writeInt(docCount);
        out.writeInt(delDocCount);
        out.writeLong(sizeInBytes);
    }
}