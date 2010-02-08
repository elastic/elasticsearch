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

package org.elasticsearch.util.io.compression;

import org.elasticsearch.util.io.Streamable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class CompressedString implements Streamable {

    private byte[] compressedString;

    private transient String string;

    CompressedString() {
    }

    public CompressedString(String string) throws IOException {
        this.string = string;
        this.compressedString = new ZipCompressor().compressString(string);
    }

    public String string() throws IOException {
        if (string != null) {
            return string;
        }
        string = new ZipCompressor().decompressString(compressedString);
        return string;
    }

    public static CompressedString readCompressedString(DataInput in) throws IOException, ClassNotFoundException {
        CompressedString result = new CompressedString();
        result.readFrom(in);
        return result;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        compressedString = new byte[in.readInt()];
        in.readFully(compressedString);
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        out.writeInt(compressedString.length);
        out.write(compressedString);
    }
}
