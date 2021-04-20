/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.upgrade.get;

import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public class UpgradeStatusRequest extends BroadcastRequest<UpgradeStatusRequest> {

    public UpgradeStatusRequest() {
        this(Strings.EMPTY_ARRAY);
    }

    public UpgradeStatusRequest(StreamInput in) throws IOException {
        super(in);
    }

    public UpgradeStatusRequest(String... indices) {
        super(indices);
    }

}
