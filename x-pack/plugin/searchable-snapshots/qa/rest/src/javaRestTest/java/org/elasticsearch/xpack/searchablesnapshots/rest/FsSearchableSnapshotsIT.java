/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots.rest;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsRestTestCase;

public class FsSearchableSnapshotsIT extends AbstractSearchableSnapshotsRestTestCase {

    @Override
    protected String writeRepositoryType() {
        return FsRepository.TYPE;
    }

    @Override
    protected Settings writeRepositorySettings() {
        return Settings.builder().put("location", System.getProperty("tests.path.repo")).build();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin", new SecureString("admin-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }
}
