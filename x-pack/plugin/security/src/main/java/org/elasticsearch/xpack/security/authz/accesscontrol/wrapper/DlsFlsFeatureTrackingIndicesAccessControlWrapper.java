/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.accesscontrol.wrapper;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;

import static org.elasticsearch.xpack.core.security.SecurityField.DOCUMENT_LEVEL_SECURITY_FEATURE;
import static org.elasticsearch.xpack.core.security.SecurityField.FIELD_LEVEL_SECURITY_FEATURE;

/**
 * The wrapper of {@link IndicesAccessControl} which adds ability to track actual Document and Field Level Security feature usage.
 */
public class DlsFlsFeatureTrackingIndicesAccessControlWrapper {

    private final XPackLicenseState licenseState;
    private final boolean isDlsFlsEnabled;

    public DlsFlsFeatureTrackingIndicesAccessControlWrapper(Settings settings, XPackLicenseState licenseState) {
        this.licenseState = licenseState;
        this.isDlsFlsEnabled = XPackSettings.DLS_FLS_ENABLED.get(settings);
    }

    public IndicesAccessControl wrap(IndicesAccessControl indicesAccessControl) {
        if (isDlsFlsEnabled == false
            || indicesAccessControl == null
            || indicesAccessControl == IndicesAccessControl.ALLOW_NO_INDICES
            || indicesAccessControl == IndicesAccessControl.DENIED
            || indicesAccessControl == IndicesAccessControl.allowAll()
            || indicesAccessControl instanceof DlsFlsFeatureUsageTracker) {
            // Wrap only if it's not one of the statically defined nor already wrapped.
            return indicesAccessControl;
        } else {
            return new DlsFlsFeatureUsageTracker(indicesAccessControl, licenseState);
        }
    }

    private static class DlsFlsFeatureUsageTracker extends IndicesAccessControl {

        private final XPackLicenseState licenseState;

        private DlsFlsFeatureUsageTracker(IndicesAccessControl wrapped, XPackLicenseState licenseState) {
            super(wrapped);
            this.licenseState = licenseState;
        }

        @Override
        public IndexAccessControl getIndexPermissions(String index) {
            final IndexAccessControl iac = super.getIndexPermissions(index);
            if (iac != null) {
                trackUsage(iac);
            }
            return iac;
        }

        private void trackUsage(IndexAccessControl iac) {
            if (iac.getDocumentPermissions().hasDocumentLevelPermissions()) {
                boolean dlsLicensed = DOCUMENT_LEVEL_SECURITY_FEATURE.check(licenseState);
                assert dlsLicensed : "DLS feature should be licensed before usage";
            }
            if (iac.getFieldPermissions().hasFieldLevelSecurity()) {
                boolean flsLicensed = FIELD_LEVEL_SECURITY_FEATURE.check(licenseState);
                assert flsLicensed : "FLS feature should be licensed before usage";
            }
        }
    }
}
