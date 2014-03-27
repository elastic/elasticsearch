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

package org.elasticsearch.common.io.stream;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import org.elasticsearch.common.text.Text;

import java.io.IOException;

/**
 *
 */
public class HandlesStreamOutput extends AdapterStreamOutput {

    private final ObjectIntOpenHashMap<String> handles = new ObjectIntOpenHashMap<>();
    private final ObjectIntOpenHashMap<Text> handlesText = new ObjectIntOpenHashMap<>();

    public HandlesStreamOutput(StreamOutput out) {
        super(out);
    }

    @Override
    public void writeSharedString(String str) throws IOException {
        if (handles.containsKey(str)) {
            out.writeByte((byte) 1);
            out.writeVInt(handles.lget());
        } else {
            int handle = handles.size();
            handles.put(str, handle);
            out.writeByte((byte) 0);
            out.writeVInt(handle);
            out.writeString(str);
        }
    }

    @Override
    public void writeString(String s) throws IOException {
        out.writeString(s);
    }

    @Override
    public void writeSharedText(Text text) throws IOException {
        if (handlesText.containsKey(text)) {
            out.writeByte((byte) 1);
            out.writeVInt(handlesText.lget());
        } else {
            int handle = handlesText.size();
            handlesText.put(text, handle);
            out.writeByte((byte) 0);
            out.writeVInt(handle);
            out.writeText(text);
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
