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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;

import java.io.IOException;
import java.util.Set;

public abstract class Condition<T> implements NamedWriteable {
    public static ObjectParser<Set<Condition>, ParseFieldMatcherSupplier> PARSER =
        new ObjectParser<>("conditions", null);
    static {
        PARSER.declareString((conditions, s) ->
            conditions.add(new MaxAge(TimeValue.parseTimeValue(s, MaxAge.NAME))), new ParseField(MaxAge.NAME));
        PARSER.declareLong((conditions, value) ->
            conditions.add(new MaxDocs(value)), new ParseField(MaxDocs.NAME));
    }

    public static class MaxAge extends Condition<TimeValue> {
        public final static String NAME = "max_age";

        public MaxAge(TimeValue value) {
            super(NAME);
            this.value = value;
        }

        public MaxAge(StreamInput in) throws IOException {
            super(NAME);
            this.value = TimeValue.timeValueMillis(in.readLong());
        }

        @Override
        public boolean matches(TimeValue value) {
            return this.value.getMillis() <= value.getMillis();
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(value.getMillis());
        }
    }

    public static class MaxDocs extends Condition<Long> {
        public final static String NAME = "max_docs";

        public MaxDocs(Long value) {
            super(NAME);
            this.value = value;
        }

        public MaxDocs(StreamInput in) throws IOException {
            super(NAME);
            this.value = in.readLong();
        }

        @Override
        public boolean matches(Long value) {
            return this.value <= value;
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

    protected T value;
    protected final String name;

    protected Condition(String name) {
        this.name = name;
    }

    public abstract boolean matches(T value);

    @Override
    public final String toString() {
        return "[" + name + ": " + value + "]";
    }
}
