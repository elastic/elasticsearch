/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.settings;

import org.elasticsearch.inference.FilteredXContent;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public abstract class FilteredXContentObject implements FilteredXContent {
    @Override
    public ToXContentObject getFilteredXContentObject() {
        return (builder, params) -> {
            builder.startObject();

            toXContentFragmentOfExposedFields(builder, params);

            builder.endObject();
            return builder;
        };
    }

    protected abstract XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, ToXContent.Params params)
        throws IOException;
}
