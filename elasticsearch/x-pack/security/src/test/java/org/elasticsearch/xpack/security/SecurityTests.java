/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

public class SecurityTests extends ESTestCase {

    private Environment env;

    @Before
    public void setupEnv() {
        Settings settings = Settings.builder()
            .put("path.home", createTempDir()).build();
        env = new Environment(settings);
    }

    public void testCustomRealmExtension() throws Exception {
        Security security = new Security(Settings.EMPTY, env);

        //security.createComponents(null, null, null, )
    }

    public void testCustomRealmExtensionConflict() throws Exception {

    }
}
