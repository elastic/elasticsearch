/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.master;

import org.elasticsearch.transport.TransportRequest;

public class MasterNodeRequestHelper {
    public static TransportRequest unwrapTermOverride(TransportRequest transportRequest) {
        return transportRequest instanceof TermOverridingMasterNodeRequest termOverridingMasterNodeRequest
            ? termOverridingMasterNodeRequest.request
            : transportRequest;
    }
}
