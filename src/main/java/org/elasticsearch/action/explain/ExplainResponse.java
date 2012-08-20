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

import java.io.IOException;

import static org.elasticsearch.common.lucene.Lucene.readExplanation;
import static org.elasticsearch.common.lucene.Lucene.writeExplanation;

/**
 * Response containing the score explanation.
 */
public class ExplainResponse implements ActionResponse {

    private Explanation explanation;
    private boolean exists;

    ExplainResponse() {
    }

    public ExplainResponse(boolean exists) {
        this.exists = exists;
    }

    public ExplainResponse(boolean exists, Explanation explanation) {
        this.exists = exists;
        this.explanation = explanation;
    }

    public Explanation getExplanation() {
        return explanation();
    }

    public Explanation explanation() {
        return explanation;
    }

    public boolean isMatch() {
        return match();
    }

    public boolean match() {
        return explanation != null && explanation.isMatch();
    }

    public boolean hasExplanation() {
        return explanation != null;
    }

    public boolean exists() {
        return exists;
    }

    public boolean isExists() {
        return exists();
    }

    public void readFrom(StreamInput in) throws IOException {
        exists = in.readBoolean();
        if (in.readBoolean()) {
            explanation = readExplanation(in);
        }
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(exists);
        if (explanation == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            writeExplanation(out, explanation);
        }
    }
}
