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

package org.elasticsearch.action.search;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.action.ValidateActions.*;

/**
 */
public class ClearScrollRequest extends ActionRequest<ClearScrollRequest> {

    private BytesReference source;

    public void setScrollIds(String... scrollIds) {
        if (scrollIds != null) {
            this.setScrollIds(Arrays.asList(scrollIds));
        }
    }

    public void setScrollIds(List<String> scrollIds) {
        if (scrollIds != null && scrollIds.isEmpty() == false) {
            try {
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.startObject();
                builder.startArray("scroll_id");
                for (String scrollId : scrollIds) {
                    builder.value(scrollId);
                }
                builder.endArray();
                builder.endObject();
                this.source(builder.string());
            } catch (Exception e) {
                throw new ElasticsearchIllegalArgumentException("Failed to build source");
            }
        }
    }

    public void scrollIds(List<String> scrollIds) {
        setScrollIds(scrollIds);
    }

    public ClearScrollRequest source(String source) {
        this.source = new BytesArray(source);
        return this;
    }

    public ClearScrollRequest source(BytesReference source) {
        this.source = source;
        return this;
    }

    public BytesReference source() {
        return source;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (source == null) {
            validationException = addValidationError("no scroll ids specified", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        source = in.readBytesReference();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBytesReference(source);
    }

}
