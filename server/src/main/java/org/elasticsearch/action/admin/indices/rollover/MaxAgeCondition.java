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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * Condition for index maximum age. Evaluates to <code>true</code>
 * when the index is at least {@link #value} old
 */
public class MaxAgeCondition extends Condition<TimeValue> {
    public static final String NAME = "max_age";

    public MaxAgeCondition(TimeValue value) {
        super(NAME);
        this.value = value;
    }

    public MaxAgeCondition(StreamInput in) throws IOException {
        super(NAME);
        this.value = TimeValue.timeValueMillis(in.readLong());
    }

    @Override
    public Result evaluate(final Stats stats) {
        long indexAge = System.currentTimeMillis() - stats.indexCreated;
        return new Result(this, this.value.getMillis() <= indexAge);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // While we technically could serialize this with out.writeTimeValue(...), that would
        // require doing the song and dance around backwards compatibility for this value. Since
        // in this case the deserialized version is not displayed to a user, it's okay to simply use
        // milliseconds. It's possible to lose precision if someone were to say, specify 50
        // nanoseconds, however, in that case, their max age is indistinguishable from 0
        // milliseconds regardless.
        out.writeLong(value.getMillis());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.field(NAME, value.getStringRep());
    }

    public static MaxAgeCondition fromXContent(XContentParser parser) throws IOException {
        if (parser.nextToken() == XContentParser.Token.VALUE_STRING) {
            return new MaxAgeCondition(TimeValue.parseTimeValue(parser.text(), NAME));
        } else {
            throw new IllegalArgumentException("invalid token: " + parser.currentToken());
        }
    }
}
