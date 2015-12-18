/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.core.License;

import java.util.Collections;
import java.util.List;

@Singleton
public class FoundLicensesService extends AbstractLifecycleComponent<FoundLicensesService> implements LicenseeRegistry, LicensesManagerService {

    @Inject
    public FoundLicensesService(Settings settings) {
        super(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void register(Licensee licensee) {
        licensee.onChange(new Licensee.Status(License.OperationMode.PLATINUM, LicenseState.ENABLED));
    }

    @Override
    protected void doStart() throws ElasticsearchException {
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

    @Override
    public List<String> licenseesWithState(LicenseState state) {
        return Collections.<String>emptyList();
    }

    @Override
    public License getLicense() {
        return null;
    }
}
