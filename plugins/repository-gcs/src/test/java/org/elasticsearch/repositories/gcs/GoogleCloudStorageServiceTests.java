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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.repositories.gcs.GoogleCloudStorageService.InternalGoogleCloudStorageService;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;

public class GoogleCloudStorageServiceTests extends ESTestCase {

    private InputStream getDummyCredentialStream() throws IOException {
        return GoogleCloudStorageServiceTests.class.getResourceAsStream("/dummy-account.json");
    }

    public void testDefaultCredential() throws Exception {
        Environment env = new Environment(Settings.builder().put("path.home", createTempDir()).build());
        GoogleCredential cred = GoogleCredential.fromStream(getDummyCredentialStream());
        InternalGoogleCloudStorageService service = new InternalGoogleCloudStorageService(env, Collections.emptyMap()) {
            @Override
            GoogleCredential getDefaultCredential() throws IOException {
                return cred;
            }
        };
        assertSame(cred, service.getCredential("default"));
    }

    public void testClientCredential() throws Exception {
        GoogleCredential cred = GoogleCredential.fromStream(getDummyCredentialStream());
        Map<String, GoogleCredential> credentials = Collections.singletonMap("clientname", cred);
        Environment env = new Environment(Settings.builder().put("path.home", createTempDir()).build());
        InternalGoogleCloudStorageService service = new InternalGoogleCloudStorageService(env, credentials);
        assertSame(cred, service.getCredential("clientname"));
    }
}
