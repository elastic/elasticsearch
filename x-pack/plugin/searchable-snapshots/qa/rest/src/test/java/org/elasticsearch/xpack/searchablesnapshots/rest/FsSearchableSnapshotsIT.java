/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.rest;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsRestTestCase;

public class FsSearchableSnapshotsIT extends AbstractSearchableSnapshotsRestTestCase {

    @Override
    protected String repositoryType() {
        return FsRepository.TYPE;
    }

    @Override
    protected Settings repositorySettings() {
        return Settings.builder().put("location", System.getProperty("tests.path.repo")).build();
    }
}
