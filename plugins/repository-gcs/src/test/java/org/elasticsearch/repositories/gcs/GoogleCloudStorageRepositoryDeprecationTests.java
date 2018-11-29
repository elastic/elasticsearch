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

package org.elasticsearch.repositories.gcs;

import com.google.cloud.storage.Storage;

import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class GoogleCloudStorageRepositoryDeprecationTests extends ESTestCase {

    public void testDeprecatedSettings() throws Exception {
        final Settings repositorySettings = Settings.builder()
            .put("bucket", "test")
            .put("application_name", "deprecated")
            .put("http.read_timeout", "10s")
            .put("http.connect_timeout", "20s")
            .build();
        final RepositoryMetaData repositoryMetaData = new RepositoryMetaData("test", "gcs", repositorySettings);
        final Environment environment = TestEnvironment.newEnvironment(Settings.builder().put("path.home", createTempDir()).build());
        new GoogleCloudStorageRepository(repositoryMetaData, environment, NamedXContentRegistry.EMPTY,
                new GoogleCloudStorageService() {
                    @Override
                    public Storage client(String clientName) throws IOException {
                        return new MockStorage("test", new ConcurrentHashMap<>());
                    }
                });

        assertWarnings(
                "Setting [application_name] in repository settings is deprecated, it must be specified in the client settings instead",
                "Setting [http.read_timeout] in repository settings is deprecated, it must be specified in the client settings instead",
                "Setting [http.connect_timeout] in repository settings is deprecated, it must be specified in the client settings instead");
    }
}
