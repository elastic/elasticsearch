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

package org.elasticsearch.index.rankeval;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Instances of this class represent a complete precision at request. They
 * encode a precision task including search intents and search specifications to
 * be executed subsequently.
 */
public class RankEvalRequest extends ActionRequest {

    /** The request data to use for evaluation. */
    private RankEvalSpec task;

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException e = null;
        if (task == null) {
            e = new ActionRequestValidationException();
            e.addValidationError("missing ranking evaluation specification");
        }
        return null;
    }

    /**
     * Returns the specification of this qa run including intents to execute,
     * specifications detailing intent translation and metrics to compute.
     */
    public RankEvalSpec getRankEvalSpec() {
        return task;
    }

    /**
     * Returns the specification of this qa run including intents to execute,
     * specifications detailing intent translation and metrics to compute.
     */
    public void setRankEvalSpec(RankEvalSpec task) {
        this.task = task;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        task = new RankEvalSpec(in);

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        task.writeTo(out);
    }
}
