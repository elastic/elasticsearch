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
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.env.BuildVersion;
import org.elasticsearch.env.NodeMetadata;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.license.ClusterStateLicenseService;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensesMetadata;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.license.internal.TrialLicenseVersion;
import org.elasticsearch.test.AbstractBootstrapCheckTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.core.XPackSettings;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class SecurityImplicitBehaviorBootstrapCheckTests extends AbstractBootstrapCheckTestCase {

    @UpdateForV9(owner = UpdateForV9.Owner.SECURITY)
    @AwaitsFix(bugUrl = "requires updates for version 9.0 bump")
    public void testUpgradeFrom8xWithImplicitSecuritySettings() throws Exception {
        final BuildVersion previousVersion = toBuildVersion(VersionUtils.randomVersionBetween(random(), Version.V_8_0_0, null));
        NodeMetadata nodeMetadata = new NodeMetadata(randomAlphaOfLength(10), previousVersion, IndexVersion.current());
        nodeMetadata = nodeMetadata.upgradeToCurrentVersion();
        ClusterStateLicenseService licenseService = mock(ClusterStateLicenseService.class);
        BootstrapCheck.BootstrapCheckResult result = new SecurityImplicitBehaviorBootstrapCheck(nodeMetadata, licenseService).check(
            createTestContext(
                Settings.EMPTY,
                createLicensesMetadata(TrialLicenseVersion.fromXContent(previousVersion.toString()), randomFrom("basic", "trial"))
            )
        );
        assertThat(result.isSuccess(), is(true));
    }

    @UpdateForV9(owner = UpdateForV9.Owner.SECURITY)
    @AwaitsFix(bugUrl = "requires updates for version 9.0 bump")
    public void testUpgradeFrom8xWithExplicitSecuritySettings() throws Exception {
        final BuildVersion previousVersion = toBuildVersion(VersionUtils.randomVersionBetween(random(), Version.V_8_0_0, null));
        NodeMetadata nodeMetadata = new NodeMetadata(randomAlphaOfLength(10), previousVersion, IndexVersion.current());
        nodeMetadata = nodeMetadata.upgradeToCurrentVersion();
        ClusterStateLicenseService licenseService = mock(ClusterStateLicenseService.class);
        BootstrapCheck.BootstrapCheckResult result = new SecurityImplicitBehaviorBootstrapCheck(nodeMetadata, licenseService).check(
            createTestContext(
                Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build(),
                createLicensesMetadata(TrialLicenseVersion.fromXContent(previousVersion.toString()), randomFrom("basic", "trial"))
            )
        );
        assertThat(result.isSuccess(), is(true));
    }

    private Metadata createLicensesMetadata(TrialLicenseVersion era, String licenseMode) throws Exception {
        License license = TestUtils.generateSignedLicense(licenseMode, TimeValue.timeValueHours(2));
        return Metadata.builder().putCustom(LicensesMetadata.TYPE, new LicensesMetadata(license, era)).build();
    }

    private static BuildVersion toBuildVersion(Version version) {
        return BuildVersion.fromVersionId(version.id());
    }
}
