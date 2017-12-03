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

package org.elasticsearch.repositories.s3;

import com.amazonaws.auth.AWSCredentials;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

public class RepositorySettingsCredentialsTests extends ESTestCase {

    public void testRepositorySettingsCredentials() {
        Settings repositorySettings = Settings.builder()
            .put(S3Repository.ACCESS_KEY_SETTING.getKey(), "aws_key")
            .put(S3Repository.SECRET_KEY_SETTING.getKey(), "aws_secret").build();
        AWSCredentials credentials = InternalAwsS3Service.buildCredentials(logger, deprecationLogger,
            S3ClientSettings.getClientSettings(Settings.EMPTY, "default"), repositorySettings).getCredentials();
        assertEquals("aws_key", credentials.getAWSAccessKeyId());
        assertEquals("aws_secret", credentials.getAWSSecretKey());
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { S3Repository.ACCESS_KEY_SETTING, S3Repository.SECRET_KEY_SETTING },
            "Using s3 access/secret key from repository settings. " +
                "Instead store these in named clients and the elasticsearch keystore for secure settings.");
    }
}
