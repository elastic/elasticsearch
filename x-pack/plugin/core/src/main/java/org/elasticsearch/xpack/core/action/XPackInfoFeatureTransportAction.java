/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.protocol.xpack.XPackInfoRequest;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.FeatureSetsInfo.FeatureSet;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

/**
 * A base class to implement {@link XPackInfoFeatureAction} actions.
 *
 * Extend this class and implement the abstract methods, and register the appropriate
 * {@link XPackInfoFeatureAction} to the subclass of this class.
 */
public abstract class XPackInfoFeatureTransportAction extends HandledTransportAction<XPackInfoRequest, XPackInfoFeatureResponse> {

    public XPackInfoFeatureTransportAction(String name, TransportService transportService, ActionFilters actionFilters) {
        super(name, transportService, actionFilters, XPackInfoRequest::new);
    }

    protected abstract String name();

    protected abstract boolean available();

    protected abstract boolean enabled();

    @Override
    protected void doExecute(Task task, XPackInfoRequest request, ActionListener<XPackInfoFeatureResponse> listener) {
        listener.onResponse(new XPackInfoFeatureResponse(new FeatureSet(name(), available(), enabled())));
    }
}
