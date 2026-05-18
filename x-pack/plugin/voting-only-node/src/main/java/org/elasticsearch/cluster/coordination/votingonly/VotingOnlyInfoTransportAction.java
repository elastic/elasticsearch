/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.cluster.coordination.votingonly;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureTransportAction;

public class VotingOnlyInfoTransportAction extends XPackInfoFeatureTransportAction {

    @Inject
    public VotingOnlyInfoTransportAction(TransportService transportService, ActionFilters actionFilters) {
        super(XPackInfoFeatureAction.VOTING_ONLY.name(), transportService, actionFilters);
    }

    @Override
    protected String name() {
        return XPackField.VOTING_ONLY;
    }

    @Override
    protected boolean available() {
        return true;
    }

    @Override
    protected boolean enabled() {
        return true;
    }
}
