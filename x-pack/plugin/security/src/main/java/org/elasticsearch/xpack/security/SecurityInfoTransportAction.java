/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureTransportAction;

/**
 * Indicates whether the features of Security are currently in use
 */
public class SecurityInfoTransportAction extends XPackInfoFeatureTransportAction {

    private final Settings settings;

    @Inject
    public SecurityInfoTransportAction(TransportService transportService, ActionFilters actionFilters, Settings settings) {
        super(XPackInfoFeatureAction.SECURITY.name(), transportService, actionFilters);
        this.settings = settings;
    }

    @Override
    public String name() {
        return XPackField.SECURITY;
    }

    @Override
    public boolean available() {
        return true;
    }

    @Override
    public boolean enabled() {
        return XPackSettings.SECURITY_ENABLED.get(settings);
    }
}
