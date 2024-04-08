/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.accesscontrol.wrapper;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl.IndexAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.DocumentPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition;
import org.mockito.Mockito;

import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.SecurityField.DOCUMENT_LEVEL_SECURITY_FEATURE;
import static org.elasticsearch.xpack.core.security.SecurityField.FIELD_LEVEL_SECURITY_FEATURE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DlsFlsFeatureTrackingIndicesAccessControlWrapperTests extends ESTestCase {

    public void testDlsFlsFeatureUsageTracking() {

        MockLicenseState licenseState = MockLicenseState.createMock();
        Mockito.when(licenseState.isAllowed(DOCUMENT_LEVEL_SECURITY_FEATURE)).thenReturn(true);
        Mockito.when(licenseState.isAllowed(FIELD_LEVEL_SECURITY_FEATURE)).thenReturn(true);
        Settings settings = Settings.builder().put(XPackSettings.DLS_FLS_ENABLED.getKey(), true).build();
        DlsFlsFeatureTrackingIndicesAccessControlWrapper wrapper = new DlsFlsFeatureTrackingIndicesAccessControlWrapper(
            settings,
            licenseState
        );

        String flsIndexName = "fls-index";
        String dlsIndexName = "dls-index";
        String dlsFlsIndexName = "dls-fls-index";
        String noDlsFlsIndexName = "no-dls-fls-restriction-index";

        FieldPermissions fieldPermissions = new FieldPermissions(
            new FieldPermissionsDefinition(new String[] { "*" }, new String[] { "private" })
        );
        DocumentPermissions documentPermissions = DocumentPermissions.filteredBy(Set.of(new BytesArray("""
            {"term":{"number":1}}""")));

        IndicesAccessControl indicesAccessControl = wrapper.wrap(
            new IndicesAccessControl(
                true,
                Map.ofEntries(
                    Map.entry(noDlsFlsIndexName, new IndexAccessControl(FieldPermissions.DEFAULT, DocumentPermissions.allowAll())),
                    Map.entry(flsIndexName, new IndexAccessControl(fieldPermissions, DocumentPermissions.allowAll())),
                    Map.entry(dlsIndexName, new IndexAccessControl(FieldPermissions.DEFAULT, documentPermissions)),
                    Map.entry(dlsFlsIndexName, new IndexAccessControl(fieldPermissions, documentPermissions))
                )
            )
        );

        // Accessing index which does not have DLS nor FLS should not track usage
        indicesAccessControl.getIndexPermissions(randomFrom(noDlsFlsIndexName, randomAlphaOfLength(3)));
        verify(licenseState, never()).featureUsed(any());

        // FLS should be tracked
        indicesAccessControl.getIndexPermissions(flsIndexName);
        verify(licenseState, times(1)).featureUsed(FIELD_LEVEL_SECURITY_FEATURE);
        verify(licenseState, times(0)).featureUsed(DOCUMENT_LEVEL_SECURITY_FEATURE);

        // DLS should be tracked
        indicesAccessControl.getIndexPermissions(dlsIndexName);
        verify(licenseState, times(1)).featureUsed(FIELD_LEVEL_SECURITY_FEATURE);
        verify(licenseState, times(1)).featureUsed(DOCUMENT_LEVEL_SECURITY_FEATURE);

        // Both DLS and FLS should be tracked
        indicesAccessControl.getIndexPermissions(dlsFlsIndexName);
        verify(licenseState, times(2)).featureUsed(FIELD_LEVEL_SECURITY_FEATURE);
        verify(licenseState, times(2)).featureUsed(DOCUMENT_LEVEL_SECURITY_FEATURE);
    }
}
