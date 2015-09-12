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

package org.elasticsearch.search.highlight;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;

import java.io.IOException;
import java.util.Arrays;

/**
 * A field highlighted with its highlighted fragments.
 */
public class HighlightField implements Streamable {

    private String name;

    private Text[] fragments;

    HighlightField() {
    }

    public HighlightField(String name, Text[] fragments) {
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
     * The name of the field highlighted.
     */
    public String getName() {
        return name();
    }

    /**
     * The highlighted fragments. <tt>null</tt> if failed to highlight (for example, the field is not stored).
     */
    public Text[] fragments() {
        return fragments;
    }

    /**
     * The highlighted fragments. <tt>null</tt> if failed to highlight (for example, the field is not stored).
     */
    public Text[] getFragments() {
        return fragments();
    }

    @Override
    public String toString() {
        return "[" + name + "], fragments[" + Arrays.toString(fragments) + "]";
    }

    public static HighlightField readHighlightField(StreamInput in) throws IOException {
        HighlightField field = new HighlightField();
        field.readFrom(in);
        return field;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        if (in.readBoolean()) {
            int size = in.readVInt();
            if (size == 0) {
                fragments = StringText.EMPTY_ARRAY;
            } else {
                fragments = new Text[size];
                for (int i = 0; i < size; i++) {
                    fragments[i] = in.readText();
                }
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        if (fragments == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVInt(fragments.length);
            for (Text fragment : fragments) {
                out.writeText(fragment);
            }
        }
    }
}
