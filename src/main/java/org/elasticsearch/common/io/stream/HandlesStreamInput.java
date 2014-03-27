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

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import org.elasticsearch.common.text.Text;

import java.io.IOException;

/**
 *
 */
public class HandlesStreamInput extends AdapterStreamInput {

    private final IntObjectOpenHashMap<String> handles = new IntObjectOpenHashMap<>();
    private final IntObjectOpenHashMap<Text> handlesText = new IntObjectOpenHashMap<>();

    HandlesStreamInput() {
        super();
    }

    public HandlesStreamInput(StreamInput in) {
        super(in);
    }

    @Override
    public String readSharedString() throws IOException {
        byte b = in.readByte();
        if (b == 0) {
            // full string with handle
            int handle = in.readVInt();
            String s = in.readString();
            handles.put(handle, s);
            return s;
        } else if (b == 1) {
            return handles.get(in.readVInt());
        } else {
            throw new IOException("Expected handle header, got [" + b + "]");
        }
    }

    @Override
    public String readString() throws IOException {
        return in.readString();
    }

    @Override
    public Text readSharedText() throws IOException {
        byte b = in.readByte();
        if (b == 0) {
            int handle = in.readVInt();
            Text s = in.readText();
            handlesText.put(handle, s);
            return s;
        } else if (b == 1) {
            return handlesText.get(in.readVInt());
        } else if (b == 2) {
            return in.readText();
        } else {
            throw new IOException("Expected handle header, got [" + b + "]");
        }
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        cleanHandles();
    }

    public void reset(StreamInput in) {
        super.reset(in);
        cleanHandles();
    }

    public void cleanHandles() {
        handles.clear();
        handlesText.clear();
    }
}
