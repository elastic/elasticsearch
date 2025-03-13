/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.fs;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.repositories.blobstore.ESFsBasedRepositoryIntegTestCase;

public class FsBlobStoreRepositoryIT extends ESFsBasedRepositoryIntegTestCase {

    @Override
    protected Settings repositorySettings(String repositoryName) {
        final Settings.Builder settings = Settings.builder().put("compress", randomBoolean()).put("location", randomRepoPath());
        if (randomBoolean()) {
            long size = 1 << randomInt(10);
            settings.put("chunk_size", ByteSizeValue.of(size, ByteSizeUnit.KB));
        }
        return settings.build();
    }
}
