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

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Condition for maximum index docs. Evaluates to <code>true</code>
 * when the index has at least {@link #value} docs
 */
public class MaxDocsCondition extends Condition<Long> {
    public final static String NAME = "max_docs";

    public MaxDocsCondition(Long value) {
        super(NAME);
        this.value = value;
    }

    public MaxDocsCondition(StreamInput in) throws IOException {
        super(NAME);
        this.value = in.readLong();
    }

    @Override
    public Result evaluate(final Stats stats) {
        return new Result(this, this.value <= stats.numDocs);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(value);
    }
}
