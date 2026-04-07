/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm.step.info;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

/**
 * An empty XContent object to indicate an ILM step is not providing any information.
 */
public final class EmptyInfo implements ToXContentObject {

    public static final EmptyInfo INSTANCE = new EmptyInfo();

    private EmptyInfo() {}

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) {
        return builder;
    }
}
