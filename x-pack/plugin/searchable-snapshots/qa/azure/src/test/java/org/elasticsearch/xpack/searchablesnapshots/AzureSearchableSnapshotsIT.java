/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.common.settings.Settings;

import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.not;

public class AzureSearchableSnapshotsIT extends AbstractSearchableSnapshotsRestTestCase {

    @Override
    protected String repositoryType() {
        return "azure";
    }

    @Override
    protected Settings repositorySettings() {
        final String container = System.getProperty("test.azure.container");
        assertThat(container, not(blankOrNullString()));

        final String basePath = System.getProperty("test.azure.base_path");
        assertThat(basePath, not(blankOrNullString()));

        return Settings.builder().put("client", "searchable_snapshots").put("container", container).put("base_path", basePath).build();
    }
}
