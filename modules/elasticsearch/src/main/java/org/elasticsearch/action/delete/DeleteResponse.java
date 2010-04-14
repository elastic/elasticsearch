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

package org.elasticsearch.action.delete;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.util.io.stream.StreamInput;
import org.elasticsearch.util.io.stream.StreamOutput;
import org.elasticsearch.util.io.stream.Streamable;

import java.io.IOException;

/**
 * The response of the delete action.
 *
 * @author kimchy (shay.banon)
 * @see org.elasticsearch.action.delete.DeleteRequest
 * @see org.elasticsearch.client.Client#delete(DeleteRequest)
 */
public class DeleteResponse implements ActionResponse, Streamable {

    private String index;

    private String id;

    private String type;

    DeleteResponse() {

    }

    DeleteResponse(String index, String type, String id) {
        this.index = index;
        this.id = id;
        this.type = type;
    }

    /**
     * The index the document was deleted from.
     */
    public String index() {
        return this.index;
    }

    /**
     * The index the document was deleted from.
     */
    public String getIndex() {
        return index;
    }

    /**
     * The type of the document deleted.
     */
    public String type() {
        return this.type;
    }

    /**
     * The type of the document deleted.
     */
    public String getType() {
        return type;
    }

    /**
     * The id of the document deleted.
     */
    public String id() {
        return this.id;
    }

    /**
     * The id of the document deleted.
     */
    public String getId() {
        return id;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        index = in.readUTF();
        id = in.readUTF();
        type = in.readUTF();
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeUTF(index);
        out.writeUTF(id);
        out.writeUTF(type);
    }
}
