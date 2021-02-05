/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureTransportAction;

public class SpatialInfoTransportAction extends XPackInfoFeatureTransportAction {

    @Inject
    public SpatialInfoTransportAction(TransportService transportService, ActionFilters actionFilters) {
        super(XPackInfoFeatureAction.SPATIAL.name(), transportService, actionFilters);
    }

    @Override
    public String name() {
        return XPackField.SPATIAL;
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
