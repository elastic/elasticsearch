/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.xpack.relevancesearch.RelevanceSearchTaskExecutor.RELEVANCE_SEARCH;

public class RelevanceSearchTaskParams implements PersistentTaskParams {

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return RELEVANCE_SEARCH;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_8_6_0;
    }

    @Override
    public void writeTo(StreamOutput out) {}
}
