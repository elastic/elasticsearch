/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.action.ActionType;

public class TransportRelayAction extends ActionType<TransportRelayResponse> {

    public static final TransportRelayAction INSTANCE = new TransportRelayAction();
    public static final String NAME = "cluster:admin/xpack/security/transport_relay";

    protected TransportRelayAction() {
        super(NAME, TransportRelayResponse::new);
    }
}
