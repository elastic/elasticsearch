/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.common.xcontent.ParseField;
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
