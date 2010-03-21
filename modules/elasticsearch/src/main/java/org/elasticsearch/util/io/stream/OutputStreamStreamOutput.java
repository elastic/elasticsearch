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

package org.elasticsearch.util.io.stream;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author kimchy (shay.banon)
 */
public class OutputStreamStreamOutput extends StreamOutput {

    private final OutputStream os;

    public OutputStreamStreamOutput(OutputStream os) {
        this.os = os;
    }

    @Override public void writeByte(byte b) throws IOException {
        os.write(b);
    }

    @Override public void writeBytes(byte[] b, int offset, int length) throws IOException {
        os.write(b, offset, length);
    }

    @Override public void reset() throws IOException {
        // nothing to do
    }

    @Override public void flush() throws IOException {
        os.flush();
    }

    @Override public void close() throws IOException {
        os.close();
    }
}
