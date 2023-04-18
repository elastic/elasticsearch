/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.xpack;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureTransportAction;

/**
 * A dummy version of xpack info for voting only.
 *
 * Serverless does not contain voting only node code. This class temporarily bridges
 * the expectation of the xpack info infrastructure which expects all xpack feature infos
 * to be available.
 */
public class DummyVotingOnlyInfoTransportAction extends XPackInfoFeatureTransportAction {
    @Inject
    public DummyVotingOnlyInfoTransportAction(TransportService transportService, ActionFilters actionFilters) {
        super(XPackInfoFeatureAction.VOTING_ONLY.name(), transportService, actionFilters);
    }

    @Override
    protected String name() {
        return XPackInfoFeatureAction.VOTING_ONLY.name();
    }

    @Override
    protected boolean available() {
        return false;
    }

    @Override
    protected boolean enabled() {
        return false;
    }
}
