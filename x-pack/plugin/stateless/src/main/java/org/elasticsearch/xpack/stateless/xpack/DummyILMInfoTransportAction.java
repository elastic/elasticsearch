/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.xpack;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureTransportAction;

public class DummyILMInfoTransportAction extends XPackInfoFeatureTransportAction {

    @Inject
    public DummyILMInfoTransportAction(TransportService transportService, ActionFilters actionFilters) {
        super(XPackInfoFeatureAction.INDEX_LIFECYCLE.name(), transportService, actionFilters);
    }

    @Override
    protected String name() {
        return XPackInfoFeatureAction.INDEX_LIFECYCLE.name();
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
