/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.enrollment;

import org.elasticsearch.common.settings.Setting;

public class EnrollmentSettings {
    private EnrollmentSettings() {
    }

    /** Setting for enabling or disabling enrollment mode. Defaults to false. */
    public static final Setting<Boolean> ENROLLMENT_ENABLED = Setting.boolSetting("cluster.enrollment.enabled", false,
        Setting.Property.NodeScope);
}
