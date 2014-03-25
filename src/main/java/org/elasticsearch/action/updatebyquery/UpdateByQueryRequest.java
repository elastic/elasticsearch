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

package org.elasticsearch.action.updatebyquery;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.replication.IndicesReplicationOperationRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Represents an update by query request.
 */
public class UpdateByQueryRequest extends IndicesReplicationOperationRequest<UpdateByQueryRequest> {

    private static final XContentType contentType = Requests.CONTENT_TYPE;

    private String[] types = Strings.EMPTY_ARRAY;
    private BulkResponseOption bulkResponseOption = BulkResponseOption.NONE;
    private String routing;
    private BytesReference source;
    private boolean sourceUnsafe;

    UpdateByQueryRequest() {

    }

    public UpdateByQueryRequest(String[] indices, String[] types) {
        this.indices = indices;
        this.types = types;
    }

    public String[] types() {
        return types;
    }

    public UpdateByQueryRequest types(String... types) {
        this.types = types;
        return this;
    }

    public UpdateByQueryRequest source(BytesReference source, boolean sourceUnsafe) {
        this.source = source;
        this.sourceUnsafe = sourceUnsafe;
        return this;
    }

    public UpdateByQueryRequest source(UpdateByQuerySourceBuilder sourceBuilder) {
        this.source = sourceBuilder.buildAsBytes(contentType);
        this.sourceUnsafe = false;
        return this;
    }

    public BytesReference source() {
        return source;
    }

    public boolean sourceUnsafe() {
        return sourceUnsafe;
    }

    public BulkResponseOption bulkResponseOptions() {
        return bulkResponseOption;
    }

    public UpdateByQueryRequest bulkResponseOptions(BulkResponseOption bulkResponseOption) {
        this.bulkResponseOption = bulkResponseOption;
        return this;
    }

    public String routing() {
        return routing;
    }

    public UpdateByQueryRequest routing(String routing) {
        this.routing = routing;
        return this;
    }

    public void beforeLocalFork() {
        if (sourceUnsafe) {
            source = source.copyBytesArray();
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (source == null) {
            validationException = addValidationError("Source is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        types = in.readStringArray();
        bulkResponseOption = BulkResponseOption.fromId(in.readByte());
        routing = in.readOptionalString();
        source = in.readBytesReference();
        sourceUnsafe = false;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(types);
        out.writeByte(bulkResponseOption.id());
        out.writeOptionalString(routing);
        out.writeBytesReference(source);
    }

}
