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

package org.elasticsearch.index.get;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class GetField implements Streamable, Iterable<Object> {

    private String name;

    private List<Object> values;

    private GetField() {

    }

    public GetField(String name, List<Object> values) {
        this.name = name;
        this.values = values;
    }

    public String name() {
        return name;
    }

    public String getName() {
        return name;
    }

    public Object value() {
        if (values != null && !values.isEmpty()) {
            return values.get(0);
        }
        return null;
    }

    public Object getValue() {
        return value();
    }

    public List<Object> values() {
        return values;
    }

    public List<Object> getValues() {
        return values;
    }

    @Override
    public Iterator<Object> iterator() {
        return values.iterator();
    }

    public static GetField readGetField(StreamInput in) throws IOException {
        GetField result = new GetField();
        result.readFrom(in);
        return result;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readUTF();
        int size = in.readVInt();
        values = new ArrayList<Object>(size);
        for (int i = 0; i < size; i++) {
            values.add(in.readGenericValue());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeUTF(name);
        out.writeVInt(values.size());
        for (Object obj : values) {
            out.writeGenericValue(obj);
        }
    }
}
