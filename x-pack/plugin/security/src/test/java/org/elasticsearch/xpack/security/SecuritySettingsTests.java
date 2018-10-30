/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.index.IndexAuditTrailField;


import static org.hamcrest.Matchers.containsString;

public class SecuritySettingsTests extends ESTestCase {

    public void testValidAutoCreateIndex() {
        Security.validateAutoCreateIndex(Settings.EMPTY);
        Security.validateAutoCreateIndex(Settings.builder().put("action.auto_create_index", true).build());
        Security.validateAutoCreateIndex(Settings.builder().put("action.auto_create_index", false).build());
        Security.validateAutoCreateIndex(Settings.builder().put("action.auto_create_index", ".security,.security-6").build());
        Security.validateAutoCreateIndex(Settings.builder().put("action.auto_create_index", ".security*").build());
        Security.validateAutoCreateIndex(Settings.builder().put("action.auto_create_index", "*s*").build());
        Security.validateAutoCreateIndex(Settings.builder().put("action.auto_create_index", ".s*").build());
        Security.validateAutoCreateIndex(Settings.builder().put("action.auto_create_index", "foo").build());
        Security.validateAutoCreateIndex(Settings.builder().put("action.auto_create_index", ".security_audit_log*").build());

        Security.validateAutoCreateIndex(Settings.builder()
                        .put("action.auto_create_index", ".security,.security-6")
                        .put(XPackSettings.AUDIT_ENABLED.getKey(), true)
                        .build());

        try {
            Security.validateAutoCreateIndex(Settings.builder()
                    .put("action.auto_create_index", ".security,.security-6")
                    .put(XPackSettings.AUDIT_ENABLED.getKey(), true)
                    .put(Security.AUDIT_OUTPUTS_SETTING.getKey(), randomFrom("index", "logfile,index"))
                    .build());
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString(IndexAuditTrailField.INDEX_NAME_PREFIX));
        }

        Security.validateAutoCreateIndex(Settings.builder()
                .put("action.auto_create_index", ".security_audit_log*,.security,.security-6")
                .put(XPackSettings.AUDIT_ENABLED.getKey(), true)
                .put(Security.AUDIT_OUTPUTS_SETTING.getKey(), randomFrom("index", "logfile,index"))
                .build());
    }
}
