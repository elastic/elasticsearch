/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ilm;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class RemoveIndexLifecyclePolicyResponse {

    public static final ParseField HAS_FAILURES_FIELD = new ParseField("has_failures");
    public static final ParseField FAILED_INDEXES_FIELD = new ParseField("failed_indexes");
    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<RemoveIndexLifecyclePolicyResponse, Void> PARSER = new ConstructingObjectParser<>(
        "change_policy_for_index_response",
        true,
        args -> new RemoveIndexLifecyclePolicyResponse((List<String>) args[0])
    );
    static {
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), FAILED_INDEXES_FIELD);
        // Needs to be declared but not used in constructing the response object
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), HAS_FAILURES_FIELD);
    }

    private final List<String> failedIndexes;

    public RemoveIndexLifecyclePolicyResponse(List<String> failedIndexes) {
        if (failedIndexes == null) {
            throw new IllegalArgumentException(FAILED_INDEXES_FIELD.getPreferredName() + " cannot be null");
        }
        this.failedIndexes = Collections.unmodifiableList(failedIndexes);
    }

    public List<String> getFailedIndexes() {
        return failedIndexes;
    }

    public boolean hasFailures() {
        return failedIndexes.isEmpty() == false;
    }

    public static RemoveIndexLifecyclePolicyResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public int hashCode() {
        return Objects.hash(failedIndexes);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        RemoveIndexLifecyclePolicyResponse other = (RemoveIndexLifecyclePolicyResponse) obj;
        return Objects.equals(failedIndexes, other.failedIndexes);
    }
}
