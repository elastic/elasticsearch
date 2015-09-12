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

package org.elasticsearch.action.indexedscripts.get;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * The response of a get script action.
 *
 * @see GetIndexedScriptRequest
 */
public class GetIndexedScriptResponse extends ActionResponse implements Iterable<GetField>, ToXContent {

    private GetResponse getResponse;

    GetIndexedScriptResponse() {
    }

    GetIndexedScriptResponse(GetResponse getResult) {
        this.getResponse = getResult;
    }

    /**
     * Does the document exists.
     */
    public boolean isExists() {
        return getResponse.isExists();
    }

    /**
     * The type of the document.
     */
    public String getScriptLang() {
        return getResponse.getType();
    }

    /**
     * The id of the document.
     */
    public String getId() {
        return getResponse.getId();
    }

    /**
     * The version of the doc.
     */
    public long getVersion() {
        return getResponse.getVersion();
    }

    /**
     * The source of the document (as a string).
     */
    public String getScript() {
        return ScriptService.getScriptFromResponse(getResponse);
    }

    @Override
    public Iterator<GetField> iterator() {
        return getResponse.iterator();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return getResponse.toXContent(builder, params);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        getResponse = GetResponse.readGetResponse(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        getResponse.writeTo(out);
    }
}
