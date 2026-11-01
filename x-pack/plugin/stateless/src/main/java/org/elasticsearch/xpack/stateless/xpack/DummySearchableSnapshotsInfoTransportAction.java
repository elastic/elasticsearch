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

/**
 * A dummy version of xpack info for searchable snapshots.
 *
 * Stateless does not contain searchable snapshots code. This class temporarily bridges
 * the expectation of the xpack info infrastructure which expects all xpack feature infos
 * to be available.
 */
public class DummySearchableSnapshotsInfoTransportAction extends XPackInfoFeatureTransportAction {

    @Inject
    public DummySearchableSnapshotsInfoTransportAction(TransportService transportService, ActionFilters actionFilters) {
        super(XPackInfoFeatureAction.SEARCHABLE_SNAPSHOTS.name(), transportService, actionFilters);
    }

    @Override
    protected String name() {
        return XPackInfoFeatureAction.SEARCHABLE_SNAPSHOTS.name();
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
