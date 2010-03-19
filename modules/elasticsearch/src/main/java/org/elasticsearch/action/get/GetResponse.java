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

import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.util.Unicode;
import org.elasticsearch.util.io.stream.StreamInput;
import org.elasticsearch.util.io.stream.StreamOutput;
import org.elasticsearch.util.io.stream.Streamable;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static com.google.common.collect.Iterators.*;
import static com.google.common.collect.Maps.*;
import static org.elasticsearch.action.get.GetField.*;
import static org.elasticsearch.util.json.Jackson.*;

/**
 * The response of a get action.
 *
 * @author kimchy (shay.banon)
 * @see GetRequest
 * @see org.elasticsearch.client.Client#get(GetRequest)
 */
public class GetResponse implements ActionResponse, Streamable, Iterable<GetField> {

    private String index;

    private String type;

    private String id;

    private boolean exists;

    private Map<String, GetField> fields;

    private byte[] source;

    GetResponse() {
    }

    GetResponse(String index, String type, String id, boolean exists, byte[] source, Map<String, GetField> fields) {
        this.index = index;
        this.type = type;
        this.id = id;
        this.exists = exists;
        this.source = source;
        this.fields = fields;
    }

    /**
     * Does the document exists.
     */
    public boolean exists() {
        return exists;
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
        if (source == null) {
            return null;
        }
        return Unicode.fromBytes(source);
    }

    /**
     * The source of the document (As a map).
     */
    @SuppressWarnings({"unchecked"})
    public Map<String, Object> sourceAsMap() throws ElasticSearchParseException {
        if (source == null) {
            return null;
        }
        try {
            return defaultObjectMapper().readValue(source, 0, source.length, Map.class);
        } catch (Exception e) {
            throw new ElasticSearchParseException("Failed to parse source to map", e);
        }
    }

    public Map<String, GetField> fields() {
        return this.fields;
    }

    public GetField field(String name) {
        return fields.get(name);
    }

    @Override public Iterator<GetField> iterator() {
        if (fields == null) {
            return emptyIterator();
        }
        return fields.values().iterator();
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        index = in.readUTF();
        type = in.readUTF();
        id = in.readUTF();
        exists = in.readBoolean();
        if (exists) {
            int size = in.readVInt();
            if (size > 0) {
                source = new byte[size];
                in.readFully(source);
            }
            size = in.readVInt();
            if (size > 0) {
                fields = newHashMapWithExpectedSize(size);
                for (int i = 0; i < size; i++) {
                    GetField field = readGetField(in);
                    fields.put(field.name(), field);
                }
            }
        }
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeUTF(index);
        out.writeUTF(type);
        out.writeUTF(id);
        out.writeBoolean(exists);
        if (exists) {
            if (source == null) {
                out.writeVInt(0);
            } else {
                out.writeVInt(source.length);
                out.writeBytes(source);
            }
            if (fields == null) {
                out.writeVInt(0);
            } else {
                out.writeVInt(fields.size());
                for (GetField field : fields.values()) {
                    field.writeTo(out);
                }
            }
        }
    }
}
