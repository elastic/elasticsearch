/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.fs.FsRepository;

import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.not;

public class URLSearchableSnapshotsIT extends AbstractSearchableSnapshotsRestTestCase {

    @Override
    protected String writeRepositoryType() {
        return FsRepository.TYPE;
    }

    @Override
    protected Settings writeRepositorySettings() {
        final String repoDirectory = System.getProperty("test.url.fs.repo.dir");
        assertThat(repoDirectory, not(blankOrNullString()));

        return Settings.builder().put("location", repoDirectory).build();
    }

    @Override
    protected boolean useReadRepository() {
        return true;
    }

    @Override
    protected String readRepositoryType() {
        return "url";
    }

    @Override
    protected Settings readRepositorySettings() {
        final String url = System.getProperty("test.url.http");
        assertThat(url, not(blankOrNullString()));

        return Settings.builder().put("url", url).build();
    }
}
