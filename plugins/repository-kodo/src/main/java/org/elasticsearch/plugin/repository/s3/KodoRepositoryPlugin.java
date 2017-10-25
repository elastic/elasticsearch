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

package org.elasticsearch.plugin.repository.s3;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.elasticsearch.SpecialPermission;
import org.elasticsearch.cloud.qiniu.InternalQiniuKodoService;
import org.elasticsearch.cloud.qiniu.QiniuKodoService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.s3.KodoRepository;

/**
 * A plugin to add a repository type that writes to and from the AWS S3.
 */
public class KodoRepositoryPlugin extends Plugin implements RepositoryPlugin {

    static {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            @Override
            public Void run() {
                try {
                    // kick jackson to do some static caching of declared members info
                    // ClientConfiguration clinit has some classloader problems
                    // TODO: fix that
                    Class.forName("com.amazonaws.ClientConfiguration");
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
                return null;
            }
        });
    }

    // overridable for tests
    protected QiniuKodoService createStorageService(Settings settings) {
        return new InternalQiniuKodoService(settings);
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(Environment env, NamedXContentRegistry namedXContentRegistry) {
        return Collections.singletonMap(KodoRepository.TYPE,
            (metadata) -> new KodoRepository(metadata, env.settings(), namedXContentRegistry, createStorageService(env.settings())));
    }

    @Override
    public List<String> getSettingsFilter() {
        return Arrays.asList(
            KodoRepository.Repository.KEY_SETTING.getKey(),
                KodoRepository.Repository.SECRET_SETTING.getKey());
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            // named s3 client configuration settings
            KodoRepository.ACCESS_KEY_SETTING,
            KodoRepository.SECRET_KEY_SETTING,
            KodoRepository.ENDPOINT_SETTING,

            // Register global cloud aws settings: cloud.aws (might have been registered in ec2 plugin)
            QiniuKodoService.KEY_SETTING,
            QiniuKodoService.SECRET_SETTING,
            QiniuKodoService.PROXY_HOST_SETTING,
            QiniuKodoService.PROXY_PORT_SETTING,
            QiniuKodoService.PROXY_USERNAME_SETTING,
            QiniuKodoService.PROXY_PASSWORD_SETTING,
            QiniuKodoService.SIGNER_SETTING,
            QiniuKodoService.REGION_SETTING,

            // Register S3 specific settings: cloud.aws.s3
            QiniuKodoService.CLOUD_Qiniu.KEY_SETTING,
            QiniuKodoService.CLOUD_Qiniu.SECRET_SETTING,
            QiniuKodoService.CLOUD_Qiniu.PROXY_HOST_SETTING,
            QiniuKodoService.CLOUD_Qiniu.PROXY_PORT_SETTING,
            QiniuKodoService.CLOUD_Qiniu.PROXY_USERNAME_SETTING,
            QiniuKodoService.CLOUD_Qiniu.PROXY_PASSWORD_SETTING,
            QiniuKodoService.CLOUD_Qiniu.SIGNER_SETTING,
            QiniuKodoService.CLOUD_Qiniu.REGION_SETTING,
            QiniuKodoService.CLOUD_Qiniu.ENDPOINT_SETTING,

            // Register S3 repositories settings: repositories.s3
            KodoRepository.Repositories.KEY_SETTING,
            KodoRepository.Repositories.SECRET_SETTING,
            KodoRepository.Repositories.BUCKET_SETTING,
            KodoRepository.Repositories.REGION_SETTING,
            KodoRepository.Repositories.ENDPOINT_SETTING,
            KodoRepository.Repositories.SERVER_SIDE_ENCRYPTION_SETTING,
            KodoRepository.Repositories.BUFFER_SIZE_SETTING,
            KodoRepository.Repositories.MAX_RETRIES_SETTING,
            KodoRepository.Repositories.CHUNK_SIZE_SETTING,
            KodoRepository.Repositories.COMPRESS_SETTING,
            KodoRepository.Repositories.STORAGE_CLASS_SETTING,
            KodoRepository.Repositories.CANNED_ACL_SETTING,
            KodoRepository.Repositories.BASE_PATH_SETTING,
            KodoRepository.Repositories.PATH_STYLE_ACCESS_SETTING);
    }
}
