/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * {@link NullRetentionPolicyConfig} is the implementation of {@link RetentionPolicyConfig} used when the user explicitly sets the
 * retention_policy to {@code null} in the _update request:
 *
 * POST _transform/some-transform/_update
 * {
 *   "retention_policy": null
 * }
 *
 * This is treated *differently* than simply omitting retention_policy from the request as it instructs the API to clear existing
 * retention_policy from some-transform.
 */
public class NullRetentionPolicyConfig implements RetentionPolicyConfig {

    public static final ParseField NAME = new ParseField("null_retention_policy");
    public static final NullRetentionPolicyConfig INSTANCE = new NullRetentionPolicyConfig();

    private NullRetentionPolicyConfig() {}

    @Override
    public ActionRequestValidationException validate(ActionRequestValidationException validationException) {
        return null;
    }

    @Override
    public void checkForDeprecations(String id, NamedXContentRegistry namedXContentRegistry, Consumer<DeprecationIssue> onDeprecation) {
        throw new UnsupportedOperationException();
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {}
}
