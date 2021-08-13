/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ilm;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.Objects;

/**
 * The current status of index lifecycle management. See {@link OperationMode} for available statuses.
 */
public class LifecycleManagementStatusResponse {

    private final OperationMode operationMode;
    private static final String OPERATION_MODE = "operation_mode";
    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<LifecycleManagementStatusResponse, Void> PARSER = new ConstructingObjectParser<>(
        OPERATION_MODE, true, a -> new LifecycleManagementStatusResponse((String) a[0]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField(OPERATION_MODE));
    }

    //package private for testing
    LifecycleManagementStatusResponse(String operationMode) {
        this.operationMode = OperationMode.fromString(operationMode);
    }

    public OperationMode getOperationMode() {
        return operationMode;
    }

    public static LifecycleManagementStatusResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LifecycleManagementStatusResponse that = (LifecycleManagementStatusResponse) o;
        return operationMode == that.operationMode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(operationMode);
    }
}
