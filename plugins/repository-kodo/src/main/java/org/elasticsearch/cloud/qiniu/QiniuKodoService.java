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

package org.elasticsearch.cloud.qiniu;

import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;

import java.util.Locale;
import java.util.function.Function;

public interface QiniuKodoService extends LifecycleComponent {

    // Legacy global AWS settings (shared between discovery-ec2 and repository-s3)
    // Each setting starting with `cloud.qiniu` also exists in discovery-ec2 project. Don't forget to update
    // the code there if you change anything here.
    /**
     * cloud.qiniu.access_key: AWS Access key. Shared with discovery-ec2 plugin
     */
    Setting<SecureString> KEY_SETTING = new Setting<>("cloud.qiniu.access_key", "", SecureString::new,
        Property.NodeScope, Property.Filtered, Property.Deprecated, Property.Shared);
    /**
     * cloud.qiniu.secret_key: AWS Secret key. Shared with discovery-ec2 plugin
     */
    Setting<SecureString> SECRET_SETTING = new Setting<>("cloud.qiniu.secret_key", "", SecureString::new,
        Property.NodeScope, Property.Filtered, Property.Deprecated, Property.Shared);
    /**
     * cloud.qiniu.protocol: Protocol for AWS API: http or https. Defaults to https. Shared with discovery-ec2 plugin
     */
    /**
     * cloud.qiniu.proxy.host: In case of proxy, define its hostname/IP. Shared with discovery-ec2 plugin
     */
    Setting<String> PROXY_HOST_SETTING = Setting.simpleString("cloud.qiniu.proxy.host",
        Property.NodeScope, Property.Deprecated, Property.Shared);
    /**
     * cloud.qiniu.proxy.port: In case of proxy, define its port. Defaults to 80. Shared with discovery-ec2 plugin
     */
    Setting<Integer> PROXY_PORT_SETTING = Setting.intSetting("cloud.qiniu.proxy.port", 80, 0, 1<<16,
        Property.NodeScope, Property.Deprecated, Property.Shared);
    /**
     * cloud.qiniu.proxy.username: In case of proxy with auth, define the username. Shared with discovery-ec2 plugin
     */
    Setting<SecureString> PROXY_USERNAME_SETTING = new Setting<>("cloud.qiniu.proxy.username", "", SecureString::new,
        Property.NodeScope, Property.Deprecated, Property.Shared);
    /**
     * cloud.qiniu.proxy.password: In case of proxy with auth, define the password. Shared with discovery-ec2 plugin
     */
    Setting<SecureString> PROXY_PASSWORD_SETTING = new Setting<>("cloud.qiniu.proxy.password", "", SecureString::new,
        Property.NodeScope, Property.Filtered, Property.Deprecated, Property.Shared);
    /**
     * cloud.qiniu.signer: If you are using an old AWS API version, you can define a Signer. Shared with discovery-ec2 plugin
     */
    Setting<String> SIGNER_SETTING = Setting.simpleString("cloud.qiniu.signer",
        Property.NodeScope, Property.Deprecated, Property.Shared);
    /**
     * cloud.qiniu.region: Region. Shared with discovery-ec2 plugin
     */
    Setting<String> REGION_SETTING = new Setting<>("cloud.qiniu.region", "", s -> s.toLowerCase(Locale.ROOT),
        Property.NodeScope, Property.Deprecated, Property.Shared);
    /**
     * cloud.qiniu.read_timeout: Socket read timeout. Shared with discovery-ec2 plugin
     */

    /**
     * Defines specific s3 settings starting with cloud.qiniu.s3.
     */
    interface CLOUD_Qiniu {
        /**
         * cloud.qiniu.s3.access_key: AWS Access key specific for S3 API calls. Defaults to cloud.qiniu.access_key.
         * @see QiniuKodoService#KEY_SETTING
         */
        Setting<SecureString> KEY_SETTING =
            new Setting<>("cloud.qiniu.s3.access_key", QiniuKodoService.KEY_SETTING, SecureString::new,
                Property.NodeScope, Property.Filtered, Property.Deprecated);
        /**
         * cloud.qiniu.s3.secret_key: AWS Secret key specific for S3 API calls. Defaults to cloud.qiniu.secret_key.
         * @see QiniuKodoService#SECRET_SETTING
         */
        Setting<SecureString> SECRET_SETTING =
            new Setting<>("cloud.qiniu.s3.secret_key", QiniuKodoService.SECRET_SETTING, SecureString::new,
                Property.NodeScope, Property.Filtered, Property.Deprecated);

        Setting<String> PROXY_HOST_SETTING =
            new Setting<>("cloud.qiniu.s3.proxy.host", QiniuKodoService.PROXY_HOST_SETTING, Function.identity(),
                Property.NodeScope, Property.Deprecated);
        /**
         * cloud.qiniu.s3.proxy.port: In case of proxy, define its port specific for S3 API calls.  Defaults to cloud.qiniu.proxy.port.
         * @see QiniuKodoService#PROXY_PORT_SETTING
         */
        Setting<Integer> PROXY_PORT_SETTING =
            new Setting<>("cloud.qiniu.s3.proxy.port", QiniuKodoService.PROXY_PORT_SETTING,
                s -> Setting.parseInt(s, 0, 1<<16, "cloud.qiniu.s3.proxy.port"), Property.NodeScope, Property.Deprecated);
        /**
         * cloud.qiniu.s3.proxy.username: In case of proxy with auth, define the username specific for S3 API calls.
         * Defaults to cloud.qiniu.proxy.username.
         * @see QiniuKodoService#PROXY_USERNAME_SETTING
         */
        Setting<SecureString> PROXY_USERNAME_SETTING =
            new Setting<>("cloud.qiniu.s3.proxy.username", QiniuKodoService.PROXY_USERNAME_SETTING, SecureString::new,
                Property.NodeScope, Property.Deprecated);
        /**
         * cloud.qiniu.s3.proxy.password: In case of proxy with auth, define the password specific for S3 API calls.
         * Defaults to cloud.qiniu.proxy.password.
         * @see QiniuKodoService#PROXY_PASSWORD_SETTING
         */
        Setting<SecureString> PROXY_PASSWORD_SETTING =
            new Setting<>("cloud.qiniu.s3.proxy.password", QiniuKodoService.PROXY_PASSWORD_SETTING, SecureString::new,
                Property.NodeScope, Property.Filtered, Property.Deprecated);
        /**
         * cloud.qiniu.s3.signer: If you are using an old AWS API version, you can define a Signer. Specific for S3 API calls.
         * Defaults to cloud.qiniu.signer.
         * @see QiniuKodoService#SIGNER_SETTING
         */
        Setting<String> SIGNER_SETTING =
            new Setting<>("cloud.qiniu.s3.signer", QiniuKodoService.SIGNER_SETTING, Function.identity(),
                Property.NodeScope, Property.Deprecated);
        /**
         * cloud.qiniu.s3.region: Region specific for S3 API calls. Defaults to cloud.qiniu.region.
         * @see QiniuKodoService#REGION_SETTING
         */
        Setting<String> REGION_SETTING =
            new Setting<>("cloud.qiniu.s3.region", QiniuKodoService.REGION_SETTING, s -> s.toLowerCase(Locale.ROOT),
                Property.NodeScope, Property.Deprecated);
        /**
         * cloud.qiniu.s3.endpoint: Endpoint. If not set, endpoint will be guessed based on region setting.
         */
        Setting<String> ENDPOINT_SETTING = Setting.simpleString("cloud.qiniu.s3.endpoint", Property.NodeScope);
        /**
         *
         */
    }

    QiniuKodoClient client(Settings repositorySettings, Integer maxRetries, boolean useThrottleRetries);
}
