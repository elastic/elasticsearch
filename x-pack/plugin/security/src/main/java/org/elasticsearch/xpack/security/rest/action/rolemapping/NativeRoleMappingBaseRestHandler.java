/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.rolemapping;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

abstract class NativeRoleMappingBaseRestHandler extends SecurityBaseRestHandler {

    private static final Logger logger = LogManager.getLogger(NativeRoleMappingBaseRestHandler.class);

    NativeRoleMappingBaseRestHandler(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    protected Exception innerCheckFeatureAvailable(RestRequest request) {
        Boolean nativeRoleMappingsEnabled = settings.getAsBoolean(NativeRoleMappingStore.NATIVE_ROLE_MAPPINGS_ENABLED, true);
        if (nativeRoleMappingsEnabled == false) {
            logger.debug(
                "Attempt to call [{} {}] but [{}] is [{}]",
                request.method(),
                request.rawPath(),
                NativeRoleMappingStore.NATIVE_ROLE_MAPPINGS_ENABLED,
                settings.get(NativeRoleMappingStore.NATIVE_ROLE_MAPPINGS_ENABLED)
            );
            return new ElasticsearchStatusException(
                "Native role mapping management is not enabled in this Elasticsearch instance",
                RestStatus.GONE
            );
        } else {
            return null;
        }
    }
}
