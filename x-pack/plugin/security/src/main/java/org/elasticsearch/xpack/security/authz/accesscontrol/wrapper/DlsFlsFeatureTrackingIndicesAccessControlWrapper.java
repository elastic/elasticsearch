/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.accesscontrol.wrapper;

import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;

import static org.elasticsearch.xpack.core.security.SecurityField.DOCUMENT_LEVEL_SECURITY_FEATURE;
import static org.elasticsearch.xpack.core.security.SecurityField.FIELD_LEVEL_SECURITY_FEATURE;

/**
 * The wrapper of {@link IndicesAccessControl} which adds ability to track actual Document and Field Level Security feature usage.
 */
public class DlsFlsFeatureTrackingIndicesAccessControlWrapper implements IndicesAccessControlWrapper {

    private final XPackLicenseState licenseState;

    public DlsFlsFeatureTrackingIndicesAccessControlWrapper(XPackLicenseState licenseState) {
        this.licenseState = licenseState;
    }

    @Override
    public IndicesAccessControl wrap(IndicesAccessControl indicesAccessControl) {
        return new DlsFlsFeatureUsageTracker(indicesAccessControl, licenseState);
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
                DOCUMENT_LEVEL_SECURITY_FEATURE.check(licenseState);
            }
            if (iac.getFieldPermissions().hasFieldLevelSecurity()) {
                FIELD_LEVEL_SECURITY_FEATURE.check(licenseState);
            }
        }
    }
}
