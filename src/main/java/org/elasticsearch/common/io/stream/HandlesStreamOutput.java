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

package org.elasticsearch.common.io.stream;

import gnu.trove.impl.Constants;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.elasticsearch.common.text.Text;

import java.io.IOException;

/**
 *
 */
public class HandlesStreamOutput extends AdapterStreamOutput {

    private final TObjectIntHashMap<String> handles = new TObjectIntHashMap<String>(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, -1);
    private final TObjectIntHashMap<Text> handlesText = new TObjectIntHashMap<Text>(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, -1);

    public HandlesStreamOutput(StreamOutput out) {
        super(out);
    }

    @Override
    public void writeSharedString(String str) throws IOException {
        int handle = handles.get(str);
        if (handle == -1) {
            handle = handles.size();
            handles.put(str, handle);
            out.writeByte((byte) 0);
            out.writeVInt(handle);
            out.writeString(str);
        } else {
            out.writeByte((byte) 1);
            out.writeVInt(handle);
        }
    }

    @Override
    public void writeString(String s) throws IOException {
        out.writeString(s);
    }

    @Override
    public void writeSharedText(Text text) throws IOException {
        int handle = handlesText.get(text);
        if (handle == -1) {
            handle = handlesText.size();
            handlesText.put(text, handle);
            out.writeByte((byte) 0);
            out.writeVInt(handle);
            out.writeText(text);
        } else {
            out.writeByte((byte) 1);
            out.writeVInt(handle);
        }
    }

    @Override
    public void reset() throws IOException {
        clear();
        if (out != null) {
            out.reset();
        }
    }

    public void clear() {
        handles.clear();
        handlesText.clear();
    }
}
