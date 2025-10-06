/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.LinkedProjectConfigService;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;

public interface CustomActionAuthorizationStep {
    boolean authorize(AuthorizationEngine.RequestInfo requestInfo);

    class Default implements CustomActionAuthorizationStep {
        @Override
        public boolean authorize(AuthorizationEngine.RequestInfo requestInfo) {
            return false;
        }
    }

    interface Factory {
        CustomActionAuthorizationStep create(Settings settings, LinkedProjectConfigService linkedProjectConfigService);

        class Default implements Factory {
            @Override
            public CustomActionAuthorizationStep create(Settings settings, LinkedProjectConfigService linkedProjectConfigService) {
                return new CustomActionAuthorizationStep.Default();
            }
        }
    }
}
