/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.eql.plugin.EqlPlugin;

import java.nio.file.Path;

public class LocalStateEqlXPackPlugin extends LocalStateCompositeXPackPlugin {

    public LocalStateEqlXPackPlugin(final Settings settings, final Path configPath) throws Exception {
        super(settings, configPath);
        LocalStateEqlXPackPlugin thisVar = this;
        plugins.add(new EqlPlugin(settings) {
            @Override
            protected XPackLicenseState getLicenseState() {
                return thisVar.getLicenseState();
            }
        });
    }
}
