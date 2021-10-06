/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.store.smb;

import org.elasticsearch.common.settings.Settings;


public class SmbNIOFSTests extends AbstractAzureFsTestCase {
    @Override
    public Settings indexSettings() {
        return Settings.builder()
                .put(super.indexSettings())
                .put("index.store.type", randomFrom("smb_simple_fs", "smb_nio_fs"))
                .build();
    }
}
