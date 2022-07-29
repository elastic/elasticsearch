/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.function.Consumer;

public interface RetentionPolicyConfig extends ToXContentObject, NamedWriteable {
    ActionRequestValidationException validate(ActionRequestValidationException validationException);

    void checkForDeprecations(String id, NamedXContentRegistry namedXContentRegistry, Consumer<DeprecationIssue> onDeprecation);
}
