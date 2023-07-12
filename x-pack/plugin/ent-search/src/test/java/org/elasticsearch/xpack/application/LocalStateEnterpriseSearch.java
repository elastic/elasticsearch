/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.application.rules.RuleQueryBuilder;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.nio.file.Path;
import java.util.List;

public class LocalStateEnterpriseSearch extends LocalStateCompositeXPackPlugin {

    private final EnterpriseSearch entSearchPlugin;
    private final XPackLicenseState licenseState;

    public LocalStateEnterpriseSearch(Settings settings, Path configPath) {
        super(settings, configPath);
        this.licenseState = MockLicenseState.createMock();

        this.entSearchPlugin = new EnterpriseSearch(settings) {
            @Override
            protected XPackLicenseState getLicenseState() {
                return licenseState;
            }
        };

        plugins.add(entSearchPlugin);
    }

    @Override
    protected XPackLicenseState getLicenseState() {
        return licenseState;
    }

    @Override
    protected void setLicenseState(XPackLicenseState licenseState) {
        // No-op
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return CollectionUtils.appendToCopy(
            super.getNamedWriteables(),
            new NamedWriteableRegistry.Entry(
                RuleQueryBuilder.class,
                RuleQueryBuilder.NAME,
                RuleQueryBuilder::new
            )
        );
    }

}
