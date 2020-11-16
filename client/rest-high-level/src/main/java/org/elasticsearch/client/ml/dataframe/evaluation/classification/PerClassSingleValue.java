/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.dataframe.evaluation.classification;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class PerClassSingleValue implements ToXContentObject {
    private static final ParseField CLASS_NAME = new ParseField("class_name");
    private static final ParseField VALUE = new ParseField("value");

    public static final ConstructingObjectParser<PerClassSingleValue, Void> PARSER =
        new ConstructingObjectParser<>("per_class_result", true, a -> new PerClassSingleValue((String) a[0], (double) a[1]));

    static {
        PARSER.declareString(constructorArg(), CLASS_NAME);
        PARSER.declareDouble(constructorArg(), VALUE);
    }

    private final String className;
    private final double value;

    public PerClassSingleValue(String className, double value) {
        this.className = Objects.requireNonNull(className);
        this.value = value;
    }

    public String getClassName() {
        return className;
    }

    public double getValue() {
        return value;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CLASS_NAME.getPreferredName(), className);
        builder.field(VALUE.getPreferredName(), value);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PerClassSingleValue that = (PerClassSingleValue) o;
        return Objects.equals(this.className, that.className)
            && this.value == that.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(className, value);
    }
}
