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

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.api.client.auth.oauth2.TokenRequest;
import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.json.GenericJson;
import com.google.api.client.json.webtoken.JsonWebSignature;
import com.google.api.client.json.webtoken.JsonWebToken;
import com.google.api.client.util.ClassInfo;
import com.google.api.client.util.Data;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.gcs.GoogleCloudStorageRepository;
import org.elasticsearch.repositories.gcs.GoogleCloudStorageService;

public class GoogleCloudStoragePlugin extends Plugin implements RepositoryPlugin {

    public static final String NAME = "repository-gcs";

    static {
        /*
         * Google HTTP client changes access levels because its silly and we
         * can't allow that on any old stack stack so we pull it here, up front,
         * so we can cleanly check the permissions for it. Without this changing
         * the permission can fail if any part of core is on the stack because
         * our plugin permissions don't allow core to "reach through" plugins to
         * change the permission. Because that'd be silly.
         */
        SpecialPermission.check();
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            // ClassInfo put in cache all the fields of a given class
            // that are annoted with @Key; at the same time it changes
            // the field access level using setAccessible(). Calling
            // them here put the ClassInfo in cache (they are never evicted)
            // before the SecurityManager is installed.
            ClassInfo.of(HttpHeaders.class, true);

            ClassInfo.of(JsonWebSignature.Header.class, false);
            ClassInfo.of(JsonWebToken.Payload.class, false);

            ClassInfo.of(TokenRequest.class, false);
            ClassInfo.of(TokenResponse.class, false);

            ClassInfo.of(GenericJson.class, false);
            ClassInfo.of(GenericUrl.class, false);

            Data.nullOf(GoogleJsonError.ErrorInfo.class);
            ClassInfo.of(GoogleJsonError.class, false);

            Data.nullOf(Bucket.Cors.class);
            ClassInfo.of(Bucket.class, false);
            ClassInfo.of(Bucket.Cors.class, false);
            ClassInfo.of(Bucket.Lifecycle.class, false);
            ClassInfo.of(Bucket.Logging.class, false);
            ClassInfo.of(Bucket.Owner.class, false);
            ClassInfo.of(Bucket.Versioning.class, false);
            ClassInfo.of(Bucket.Website.class, false);

            ClassInfo.of(StorageObject.class, false);
            ClassInfo.of(StorageObject.Owner.class, false);

            ClassInfo.of(Objects.class, false);

            ClassInfo.of(Storage.Buckets.Get.class, false);
            ClassInfo.of(Storage.Buckets.Insert.class, false);

            ClassInfo.of(Storage.Objects.Get.class, false);
            ClassInfo.of(Storage.Objects.Insert.class, false);
            ClassInfo.of(Storage.Objects.Delete.class, false);
            ClassInfo.of(Storage.Objects.Copy.class, false);
            ClassInfo.of(Storage.Objects.List.class, false);

            return null;
        });
    }

    private final Map<String, GoogleCredential> credentials;

    public GoogleCloudStoragePlugin(Settings settings) {
        credentials = GoogleCloudStorageService.loadClientCredentials(settings);
    }

    // overridable for tests
    protected GoogleCloudStorageService createStorageService(Environment environment) {
        return new GoogleCloudStorageService.InternalGoogleCloudStorageService(environment, credentials);
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(Environment env, NamedXContentRegistry namedXContentRegistry) {
        return Collections.singletonMap(GoogleCloudStorageRepository.TYPE,
            (metadata) -> new GoogleCloudStorageRepository(metadata, env, namedXContentRegistry, createStorageService(env)));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Collections.singletonList(GoogleCloudStorageService.CREDENTIALS_FILE_SETTING);
    }
}
