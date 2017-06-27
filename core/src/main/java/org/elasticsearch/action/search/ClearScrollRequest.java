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

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class ClearScrollRequest extends ActionRequest implements ToXContentObject {

    private List<String> scrollIds;

    public List<String> getScrollIds() {
        return scrollIds;
    }

    public void setScrollIds(List<String> scrollIds) {
        this.scrollIds = scrollIds;
    }

    public void addScrollId(String scrollId) {
        if (scrollIds == null) {
            scrollIds = new ArrayList<>();
        }
        scrollIds.add(scrollId);
    }

    public List<String> scrollIds() {
        return scrollIds;
    }

    public void scrollIds(List<String> scrollIds) {
        this.scrollIds = scrollIds;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (scrollIds == null || scrollIds.isEmpty()) {
            validationException = addValidationError("no scroll ids specified", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        scrollIds = Arrays.asList(in.readStringArray());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (scrollIds == null) {
            out.writeVInt(0);
        } else {
            out.writeStringArray(scrollIds.toArray(new String[scrollIds.size()]));
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("scroll_id");
        for (String scrollId : scrollIds) {
            builder.value(scrollId);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public void fromXContent(XContentParser parser) throws IOException {
        scrollIds = null;
        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("Malformed content, must start with an object");
        } else {
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if ("scroll_id".equals(currentFieldName)){
                    if (token == XContentParser.Token.START_ARRAY) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token.isValue() == false) {
                                throw new IllegalArgumentException("scroll_id array element should only contain scroll_id");
                            }
                            addScrollId(parser.text());
                        }
                    } else {
                        if (token.isValue() == false) {
                            throw new IllegalArgumentException("scroll_id element should only contain scroll_id");
                        }
                        addScrollId(parser.text());
                    }
                } else {
                    throw new IllegalArgumentException("Unknown parameter [" + currentFieldName
                            + "] in request body or parameter is of the wrong type[" + token + "] ");
                }
            }
        }
    }
}
