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
package org.elasticsearch.client.ml;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Abstract class that provides a list of results and their count.
 */
public abstract class AbstractResultResponse<T extends ToXContent> implements ToXContentObject {

    public static final ParseField COUNT = new ParseField("count");

    private final ParseField resultsField;
    protected final List<T> results;
    protected final long count;

    AbstractResultResponse(ParseField resultsField, List<T> results, long count) {
        this.resultsField = Objects.requireNonNull(resultsField,
            "[results_field] must not be null");
        this.results = Collections.unmodifiableList(results);
        this.count = count;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(COUNT.getPreferredName(), count);
        builder.field(resultsField.getPreferredName(), results);
        builder.endObject();
        return builder;
    }

    public long count() {
        return count;
    }
}
