/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.transport.TransportRequestOptions;

public class BulkAction extends ActionType<BulkResponse> {

    public static final BulkAction INSTANCE = new BulkAction();
    public static final String NAME = "indices:data/write/bulk";

    private static final TransportRequestOptions TRANSPORT_REQUEST_OPTIONS = TransportRequestOptions.of(
        null,
        TransportRequestOptions.Type.BULK
    );

    private BulkAction() {
        super(NAME, BulkResponse::new);
    }

}
