/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.Build;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class RecoverySettingsTests extends ESTestCase {
    public void testEnabledSettingRegisteredInSnapshotBuilds() {
        final Build currentBuild = Build.CURRENT;

        assertThat(currentBuild.isSnapshot(), is(equalTo(true)));
        assertThat(RecoverySettings.featureFlagEnabledSettings(), hasItem(RecoverySettings.INDICES_RECOVERY_USE_SNAPSHOTS_SETTING));
    }

    public void testEnabledSettingNotRegisteredInNonSnapshotBuilds() {
        Build build =
            new Build(Build.Flavor.DEFAULT, Build.Type.UNKNOWN, "unknown", "unknown", false, "version");

        assertThat(build.isSnapshot(), is(equalTo(false)));
        assertThat(RecoverySettings.featureFlagEnabledSettings(build),
            not(hasItem(RecoverySettings.INDICES_RECOVERY_USE_SNAPSHOTS_SETTING)));
    }
}
