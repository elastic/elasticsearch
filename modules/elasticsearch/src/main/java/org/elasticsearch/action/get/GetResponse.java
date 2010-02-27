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

package org.elasticsearch.action.get;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.util.Unicode;
import org.elasticsearch.util.io.Streamable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * The response of a get action.
 *
 * @author kimchy (shay.banon)
 * @see GetRequest
 * @see org.elasticsearch.client.Client#get(GetRequest)
 */
public class GetResponse implements ActionResponse, Streamable {

    private String index;

    private String type;

    private String id;

    private byte[] source;

    GetResponse() {
    }

    GetResponse(String index, String type, String id, byte[] source) {
        this.index = index;
        this.type = type;
        this.id = id;
        this.source = source;
    }

    /**
     * Does the document exists.
     */
    public boolean exists() {
        return source == null;
    }

    /**
     * The index the document was fetched from.
     */
    public String index() {
        return this.index;
    }

    /**
     * The type of the document.
     */
    public String type() {
        return type;
    }

    /**
     * The id of the document.
     */
    public String id() {
        return id;
    }

    /**
     * The source of the document if exists.
     */
    public byte[] source() {
        return this.source;
    }

    /**
     * The source of the document (as a string).
     */
    public String sourceAsString() {
        return Unicode.fromBytes(source);
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        index = in.readUTF();
        type = in.readUTF();
        id = in.readUTF();
        int size = in.readInt();
        if (size > 0) {
            source = new byte[size];
            in.readFully(source);
        }
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        out.writeUTF(index);
        out.writeUTF(type);
        out.writeUTF(id);
        if (source == null) {
            out.writeInt(0);
        } else {
            out.writeInt(source.length);
            out.write(source);
        }
    }
}
