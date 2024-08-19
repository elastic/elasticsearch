/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;

public class DataStreamInfoTransportAction extends XPackInfoFeatureTransportAction {

    @Inject
    public DataStreamInfoTransportAction(TransportService transportService, ActionFilters actionFilters) {
        super(XPackInfoFeatureAction.DATA_STREAMS.name(), transportService, actionFilters);
    }

    @Override
    public String name() {
        return XPackField.DATA_STREAMS;
    }

    @Override
    public boolean available() {
        return true;
    }

    @Override
    public boolean enabled() {
        return true;
    }

}
