/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public class LifecyclePolicy implements ToXContentObject {

    public static final ParseField PHASES_FIELD = new ParseField("phases");

    private List<Phase> phases;

    public LifecyclePolicy(List<Phase> phases) {
        this.phases = phases;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.array(PHASES_FIELD.getPreferredName(), phases);
        builder.endObject();
        return builder;
    }
}
