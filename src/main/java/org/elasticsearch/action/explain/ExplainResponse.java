/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.action.explain;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.get.GetResult;

import java.io.IOException;

import static org.elasticsearch.common.lucene.Lucene.readExplanation;
import static org.elasticsearch.common.lucene.Lucene.writeExplanation;

/**
 * Response containing the score explanation.
 */
public class ExplainResponse extends ActionResponse {

    private boolean exists;
    private Explanation explanation;
    private GetResult getResult;

    ExplainResponse() {
    }

    public ExplainResponse(boolean exists) {
        this.exists = exists;
    }

    public ExplainResponse(boolean exists, Explanation explanation) {
        this.exists = exists;
        this.explanation = explanation;
    }

    public ExplainResponse(boolean exists, Explanation explanation, GetResult getResult) {
        this.exists = exists;
        this.explanation = explanation;
        this.getResult = getResult;
    }

    public Explanation getExplanation() {
        return explanation;
    }

    public boolean isMatch() {
        return explanation != null && explanation.isMatch();
    }

    public boolean hasExplanation() {
        return explanation != null;
    }

    public boolean isExists() {
        return exists;
    }

    public GetResult getGetResult() {
        return getResult;
    }

    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        exists = in.readBoolean();
        if (in.readBoolean()) {
            explanation = readExplanation(in);
        }
        if (in.readBoolean()) {
            getResult = GetResult.readGetResult(in);
        }
    }

    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(exists);
        if (explanation == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            writeExplanation(out, explanation);
        }
        if (getResult == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            getResult.writeTo(out);
        }
    }
}
