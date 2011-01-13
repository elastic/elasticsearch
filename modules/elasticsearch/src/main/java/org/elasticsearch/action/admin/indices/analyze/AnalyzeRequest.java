/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.analyze;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.single.custom.SingleCustomOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.Actions.*;

/**
 * A request to analyze a text associated with a specific index. Allow to provide
 * the actual analyzer name to perform the analysis with.
 *
 * @author kimchy
 */
public class AnalyzeRequest extends SingleCustomOperationRequest {

    private String index;

    private String text;

    private String analyzer;

    AnalyzeRequest() {

    }

    /**
     * Constructs a new analyzer request for the provided index and text.
     *
     * @param index The index name
     * @param text  The text to analyze
     */
    public AnalyzeRequest(String index, String text) {
        this.index = index;
        this.text = text;
    }

    public String text() {
        return this.text;
    }

    public AnalyzeRequest index(String index) {
        this.index = index;
        return this;
    }

    public String index() {
        return this.index;
    }

    public AnalyzeRequest analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    public String analyzer() {
        return this.analyzer;
    }

    /**
     * if this operation hits a node with a local relevant shard, should it be preferred
     * to be executed on, or just do plain round robin. Defaults to <tt>true</tt>
     */
    @Override public AnalyzeRequest preferLocal(boolean preferLocal) {
        super.preferLocal(preferLocal);
        return this;
    }

    @Override public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (index == null) {
            validationException = addValidationError("index is missing", validationException);
        }
        if (text == null) {
            validationException = addValidationError("text is missing", validationException);
        }
        return validationException;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        index = in.readUTF();
        text = in.readUTF();
        if (in.readBoolean()) {
            analyzer = in.readUTF();
        }
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeUTF(index);
        out.writeUTF(text);
        if (analyzer == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(analyzer);
        }
    }
}
