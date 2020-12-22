/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureTransportAction;

public class RuntimeFieldsInfoTransportAction extends XPackInfoFeatureTransportAction {
    @Inject
    public RuntimeFieldsInfoTransportAction(TransportService transportService, ActionFilters actionFilters) {
        super(XPackInfoFeatureAction.RUNTIME_FIELDS.name(), transportService, actionFilters);
    }

    @Override
    protected String name() {
        return XPackField.RUNTIME_FIELDS;
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
