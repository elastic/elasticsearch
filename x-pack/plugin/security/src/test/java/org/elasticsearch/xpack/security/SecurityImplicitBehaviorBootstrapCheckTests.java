/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.Version;
import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.NodeMetadata;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.license.ClusterStateLicenseService;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensesMetadata;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.test.AbstractBootstrapCheckTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.core.XPackSettings;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SecurityImplicitBehaviorBootstrapCheckTests extends AbstractBootstrapCheckTestCase {

    public void testFailureUpgradeFrom7xWithImplicitSecuritySettings() throws Exception {
        final Version previousVersion = randomValueOtherThan(
            Version.V_8_0_0,
            () -> VersionUtils.randomVersionBetween(random(), Version.CURRENT.minimumCompatibilityVersion(), Version.V_8_0_0)
        );
        NodeMetadata nodeMetadata = new NodeMetadata(randomAlphaOfLength(10), previousVersion, IndexVersion.CURRENT);
        nodeMetadata = nodeMetadata.upgradeToCurrentVersion();
        ClusterStateLicenseService licenseService = mock(ClusterStateLicenseService.class);
        Metadata metadata = createLicensesMetadata(previousVersion, randomFrom("basic", "trial"));
        License license = mock(License.class);
        when(licenseService.getLicense(metadata)).thenReturn(license);
        when(license.operationMode()).thenReturn(randomFrom(License.OperationMode.BASIC, License.OperationMode.TRIAL));
        BootstrapCheck.BootstrapCheckResult result = new SecurityImplicitBehaviorBootstrapCheck(nodeMetadata, licenseService).check(
            createTestContext(Settings.EMPTY, metadata)
        );
        assertThat(result.isFailure(), is(true));
        assertThat(
            result.getMessage(),
            equalTo(
                "The default value for ["
                    + XPackSettings.SECURITY_ENABLED.getKey()
                    + "] has changed in the current version. "
                    + " Security features were implicitly disabled for this node but they would now be enabled, possibly"
                    + " preventing access to the node. "
                    + "See https://www.elastic.co/guide/en/elasticsearch/reference/"
                    + Version.CURRENT.major
                    + "."
                    + Version.CURRENT.minor
                    + "/security-minimal-setup.html to configure security, or explicitly disable security by "
                    + "setting [xpack.security.enabled] to \"false\" in elasticsearch.yml before restarting the node."
            )
        );
    }

    public void testUpgradeFrom7xWithImplicitSecuritySettingsOnGoldPlus() throws Exception {
        final Version previousVersion = randomValueOtherThan(
            Version.V_8_0_0,
            () -> VersionUtils.randomVersionBetween(random(), Version.CURRENT.minimumCompatibilityVersion(), Version.V_8_0_0)
        );
        NodeMetadata nodeMetadata = new NodeMetadata(randomAlphaOfLength(10), previousVersion, IndexVersion.CURRENT);
        nodeMetadata = nodeMetadata.upgradeToCurrentVersion();
        ClusterStateLicenseService licenseService = mock(ClusterStateLicenseService.class);
        Metadata metadata = createLicensesMetadata(previousVersion, randomFrom("gold", "platinum"));
        License license = mock(License.class);
        when(licenseService.getLicense(metadata)).thenReturn(license);
        when(license.operationMode()).thenReturn(randomFrom(License.OperationMode.GOLD, License.OperationMode.PLATINUM));
        BootstrapCheck.BootstrapCheckResult result = new SecurityImplicitBehaviorBootstrapCheck(nodeMetadata, licenseService).check(
            createTestContext(Settings.EMPTY, metadata)
        );
        assertThat(result.isSuccess(), is(true));
    }

    public void testUpgradeFrom7xWithExplicitSecuritySettings() throws Exception {
        final Version previousVersion = randomValueOtherThan(
            Version.V_8_0_0,
            () -> VersionUtils.randomVersionBetween(random(), Version.CURRENT.minimumCompatibilityVersion(), Version.V_8_0_0)
        );
        NodeMetadata nodeMetadata = new NodeMetadata(randomAlphaOfLength(10), previousVersion, IndexVersion.CURRENT);
        nodeMetadata = nodeMetadata.upgradeToCurrentVersion();
        ClusterStateLicenseService licenseService = mock(ClusterStateLicenseService.class);
        BootstrapCheck.BootstrapCheckResult result = new SecurityImplicitBehaviorBootstrapCheck(nodeMetadata, licenseService).check(
            createTestContext(
                Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build(),
                createLicensesMetadata(previousVersion, randomFrom("basic", "trial"))
            )
        );
        assertThat(result.isSuccess(), is(true));
    }

    public void testUpgradeFrom8xWithImplicitSecuritySettings() throws Exception {
        final Version previousVersion = VersionUtils.randomVersionBetween(random(), Version.V_8_0_0, null);
        NodeMetadata nodeMetadata = new NodeMetadata(randomAlphaOfLength(10), previousVersion, IndexVersion.CURRENT);
        nodeMetadata = nodeMetadata.upgradeToCurrentVersion();
        ClusterStateLicenseService licenseService = mock(ClusterStateLicenseService.class);
        BootstrapCheck.BootstrapCheckResult result = new SecurityImplicitBehaviorBootstrapCheck(nodeMetadata, licenseService).check(
            createTestContext(Settings.EMPTY, createLicensesMetadata(previousVersion, randomFrom("basic", "trial")))
        );
        assertThat(result.isSuccess(), is(true));
    }

    public void testUpgradeFrom8xWithExplicitSecuritySettings() throws Exception {
        final Version previousVersion = VersionUtils.randomVersionBetween(random(), Version.V_8_0_0, null);
        NodeMetadata nodeMetadata = new NodeMetadata(randomAlphaOfLength(10), previousVersion, IndexVersion.CURRENT);
        nodeMetadata = nodeMetadata.upgradeToCurrentVersion();
        ClusterStateLicenseService licenseService = mock(ClusterStateLicenseService.class);
        BootstrapCheck.BootstrapCheckResult result = new SecurityImplicitBehaviorBootstrapCheck(nodeMetadata, licenseService).check(
            createTestContext(
                Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build(),
                createLicensesMetadata(previousVersion, randomFrom("basic", "trial"))
            )
        );
        assertThat(result.isSuccess(), is(true));
    }

    private Metadata createLicensesMetadata(Version version, String licenseMode) throws Exception {
        License license = TestUtils.generateSignedLicense(licenseMode, TimeValue.timeValueHours(2));
        return Metadata.builder().putCustom(LicensesMetadata.TYPE, new LicensesMetadata(license, version)).build();
    }
}
