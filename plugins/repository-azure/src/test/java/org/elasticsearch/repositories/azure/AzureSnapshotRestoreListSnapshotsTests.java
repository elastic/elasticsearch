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

package org.elasticsearch.repositories.azure;

import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cloud.azure.AbstractAzureWithThirdPartyIntegTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.azure.AzureRepository.Repository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * This test needs Azure to run and -Dtests.thirdparty=true to be set
 * and -Dtests.config=/path/to/elasticsearch.yml
 * @see AbstractAzureWithThirdPartyIntegTestCase
 */
@ClusterScope(
        scope = ESIntegTestCase.Scope.SUITE,
        supportsDedicatedMasters = false, numDataNodes = 1,
        transportClientRatio = 0.0)
public class AzureSnapshotRestoreListSnapshotsTests extends AbstractAzureWithThirdPartyIntegTestCase {
    public void testList() {
        Client client = client();
        logger.info("-->  creating azure primary repository");
        PutRepositoryResponse putRepositoryResponsePrimary = client.admin().cluster().preparePutRepository("primary")
                .setType("azure").setSettings(Settings.builder()
                        .put(Repository.ACCOUNT_SETTING.getKey(), "my_account")
                        .put(Repository.CONTAINER_SETTING.getKey(), "container")
                ).get();
        assertThat(putRepositoryResponsePrimary.isAcknowledged(), equalTo(true));
        logger.info("-->  creating azure secondary repository");
        PutRepositoryResponse putRepositoryResponseSecondary = client.admin().cluster().preparePutRepository("secondary")
                .setType("azure").setSettings(Settings.builder()
                        .put(Repository.ACCOUNT_SETTING.getKey(), "my_account")
                        .put(Repository.CONTAINER_SETTING.getKey(), "container")
                        .put(Repository.LOCATION_MODE_SETTING.getKey(), "secondary_only")
                ).get();
        assertThat(putRepositoryResponseSecondary.isAcknowledged(), equalTo(true));


        logger.info("--> start get snapshots on primary");
        long startWait = System.currentTimeMillis();
        client.admin().cluster().prepareGetSnapshots("primary").get();
        long endWait = System.currentTimeMillis();
        // definitely should be done in 30s, and if its not working as expected, it takes over 1m
        assertThat(endWait - startWait, lessThanOrEqualTo(30000L));

        logger.info("--> start get snapshots on secondary");
        startWait = System.currentTimeMillis();
        client.admin().cluster().prepareGetSnapshots("secondary").get();
        endWait = System.currentTimeMillis();
        logger.info("--> end of get snapshots on secondary. Took {} ms", endWait - startWait);
        assertThat(endWait - startWait, lessThanOrEqualTo(30000L));
    }
}
