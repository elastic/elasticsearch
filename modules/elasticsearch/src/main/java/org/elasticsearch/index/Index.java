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

package org.elasticsearch.index;

import org.elasticsearch.util.concurrent.Immutable;
import org.elasticsearch.util.io.Streamable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * @author kimchy (Shay Banon)
 */
@Immutable
public class Index implements Serializable, Streamable {

    private String name;

    private Index() {

    }

    public Index(String name) {
        this.name = name;
    }

    public String name() {
        return this.name;
    }

    @Override public String toString() {
        return "Index [" + name + "]";
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Index index1 = (Index) o;

        if (name != null ? !name.equals(index1.name) : index1.name != null) return false;

        return true;
    }

    @Override public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    public static Index readIndexName(DataInput in) throws IOException, ClassNotFoundException {
        Index index = new Index();
        index.readFrom(in);
        return index;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        name = in.readUTF();
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        out.writeUTF(name);
    }
}
