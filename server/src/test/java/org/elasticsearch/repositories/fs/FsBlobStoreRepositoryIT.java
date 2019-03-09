/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.repositories.fs;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.instanceOf;

public class FsBlobStoreRepositoryIT extends ESBlobStoreRepositoryIntegTestCase {
    @Override
    protected void createTestRepository(String name, boolean verify) {
        assertAcked(client().admin().cluster().preparePutRepository(name)
            .setVerify(verify)
            .setType("fs").setSettings(Settings.builder()
                .put("location", randomRepoPath())
                .put("compress", randomBoolean())
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));
    }

    @Override
    protected void afterCreationCheck(Repository repository) {
        assertThat(repository, instanceOf(FsRepository.class));
    }
}
