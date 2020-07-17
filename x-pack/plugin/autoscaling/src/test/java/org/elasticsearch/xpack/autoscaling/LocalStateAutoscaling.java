/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

public class LocalStateAutoscaling extends LocalStateCompositeXPackPlugin {

    public LocalStateAutoscaling(final Settings settings) {
        super(settings, null);
        plugins.add(new Autoscaling(settings) {

            @Override
            protected XPackLicenseState getLicenseState() {
                return LocalStateAutoscaling.this.getLicenseState();
            }

        });
    }

}
