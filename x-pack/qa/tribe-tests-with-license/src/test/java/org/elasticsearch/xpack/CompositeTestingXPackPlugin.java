/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.deprecation.Deprecation;
import org.elasticsearch.xpack.graph.Graph;
import org.elasticsearch.xpack.logstash.Logstash;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.monitoring.Monitoring;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.watcher.Watcher;

import java.nio.file.Path;

public class CompositeTestingXPackPlugin extends LocalStateCompositeXPackPlugin {

    public CompositeTestingXPackPlugin(final Settings settings, final Path configPath) throws Exception {
        super(settings, configPath);
        CompositeTestingXPackPlugin thisVar = this;
        plugins.add(new Deprecation());
        plugins.add(new Graph(settings));
        plugins.add(new Logstash(settings));
        plugins.add(new MachineLearning(settings, configPath) {
            @Override
            protected XPackLicenseState getLicenseState() {
                return super.getLicenseState();
            }
        });
        plugins.add(new Monitoring(settings) {
            @Override
            protected SSLService getSslService() {
                return thisVar.getSslService();
            }

            @Override
            protected LicenseService getLicenseService() {
                return thisVar.getLicenseService();
            }

            @Override
            protected XPackLicenseState getLicenseState() {
                return thisVar.getLicenseState();
            }
        });
        plugins.add(new Watcher(settings) {
            @Override
            protected SSLService getSslService() {
                return thisVar.getSslService();
            }

            @Override
            protected XPackLicenseState getLicenseState() {
                return thisVar.getLicenseState();
            }
        });
        plugins.add(new Security(settings, configPath) {
            @Override
            protected SSLService getSslService() { return thisVar.getSslService(); }

            @Override
            protected XPackLicenseState getLicenseState() { return thisVar.getLicenseState(); }
        });
    }
}
