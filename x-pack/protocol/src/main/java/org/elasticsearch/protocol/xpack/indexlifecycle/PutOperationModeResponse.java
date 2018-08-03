/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.protocol.xpack.indexlifecycle;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentParser;

public class PutOperationModeResponse extends AcknowledgedResponse implements ToXContentObject {

    public static PutOperationModeResponse fromXContent(XContentParser parser) {
        return new PutOperationModeResponse(parseAcknowledged(parser));
    }

    public PutOperationModeResponse() {
    }

    public PutOperationModeResponse(boolean acknowledged) {
        super(acknowledged);
    }
}