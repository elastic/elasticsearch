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

package org.elasticsearch.index;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.io.Serializable;

/**
 *
 */
public class Index implements Serializable, Streamable {

    private String name;

    private Index() {

    }

    public Index(String name) {
        this.name = name.intern();
    }

    public String name() {
        return this.name;
    }

    public String getName() {
        return name();
    }

    @Override
    public String toString() {
        return "[" + name + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        Index index1 = (Index) o;
        return name.equals(index1.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    public static Index readIndexName(StreamInput in) throws IOException {
        Index index = new Index();
        index.readFrom(in);
        return index;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString().intern();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
    }
}
