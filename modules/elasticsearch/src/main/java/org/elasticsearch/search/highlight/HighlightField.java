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

package org.elasticsearch.search.highlight;

import org.elasticsearch.util.Strings;
import org.elasticsearch.util.io.Streamable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * A field highlighted with its higlighted fragments.
 *
 * @author kimchy (shay.banon)
 */
public class HighlightField implements Streamable {

    private String name;

    private String[] fragments;

    HighlightField() {
    }

    public HighlightField(String name, String[] fragments) {
        this.name = name;
        this.fragments = fragments;
    }

    /**
     * The name of the field highlighted.
     */
    public String name() {
        return name;
    }

    /**
     * The highlighted fragments. <tt>null</tt> if failed to highlight (for example, the field is not stored).
     */
    public String[] fragments() {
        return fragments;
    }

    @Override public String toString() {
        return "[" + name + "], fragments[" + Arrays.toString(fragments) + "]";
    }

    public static HighlightField readHighlightField(DataInput in) throws IOException, ClassNotFoundException {
        HighlightField field = new HighlightField();
        field.readFrom(in);
        return field;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        name = in.readUTF();
        if (in.readBoolean()) {
            int size = in.readInt();
            if (size == 0) {
                fragments = Strings.EMPTY_ARRAY;
            } else {
                fragments = new String[size];
                for (int i = 0; i < size; i++) {
                    fragments[i] = in.readUTF();
                }
            }
        }
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        out.writeUTF(name);
        if (fragments == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeInt(fragments.length);
            for (String fragment : fragments) {
                out.writeUTF(fragment);
            }
        }
    }
}
