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

package org.elasticsearch.cloud.aws;

import com.amazonaws.Protocol;
import com.amazonaws.services.s3.AmazonS3;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;

import java.util.Locale;
import java.util.function.Function;

/**
 *
 */
public interface AwsS3Service extends LifecycleComponent<AwsS3Service> {

    // Global AWS settings (shared between discovery-ec2 and repository-s3)
    // Each setting starting with `cloud.aws` also exists in discovery-ec2 project. Don't forget to update
    // the code there if you change anything here.
    /**
     * cloud.aws.access_key: AWS Access key. Shared with discovery-ec2 plugin
     */
    Setting<String> KEY_SETTING =
        Setting.simpleString("cloud.aws.access_key", Property.NodeScope, Property.Filtered, Property.Shared);
    /**
     * cloud.aws.secret_key: AWS Secret key. Shared with discovery-ec2 plugin
     */
    Setting<String> SECRET_SETTING =
        Setting.simpleString("cloud.aws.secret_key", Property.NodeScope, Property.Filtered, Property.Shared);
    /**
     * cloud.aws.protocol: Protocol for AWS API: http or https. Defaults to https. Shared with discovery-ec2 plugin
     */
    Setting<Protocol> PROTOCOL_SETTING = new Setting<>("cloud.aws.protocol", "https", s -> Protocol.valueOf(s.toUpperCase(Locale.ROOT)),
        Property.NodeScope, Property.Shared);
    /**
     * cloud.aws.proxy.host: In case of proxy, define its hostname/IP. Shared with discovery-ec2 plugin
     */
    Setting<String> PROXY_HOST_SETTING = Setting.simpleString("cloud.aws.proxy.host", Property.NodeScope, Property.Shared);
    /**
     * cloud.aws.proxy.port: In case of proxy, define its port. Defaults to 80. Shared with discovery-ec2 plugin
     */
    Setting<Integer> PROXY_PORT_SETTING = Setting.intSetting("cloud.aws.proxy.port", 80, 0, 1<<16, Property.NodeScope,
        Property.Shared);
    /**
     * cloud.aws.proxy.username: In case of proxy with auth, define the username. Shared with discovery-ec2 plugin
     */
    Setting<String> PROXY_USERNAME_SETTING = Setting.simpleString("cloud.aws.proxy.username", Property.NodeScope, Property.Shared);
    /**
     * cloud.aws.proxy.password: In case of proxy with auth, define the password. Shared with discovery-ec2 plugin
     */
    Setting<String> PROXY_PASSWORD_SETTING =
        Setting.simpleString("cloud.aws.proxy.password", Property.NodeScope, Property.Filtered, Property.Shared);
    /**
     * cloud.aws.signer: If you are using an old AWS API version, you can define a Signer. Shared with discovery-ec2 plugin
     */
    Setting<String> SIGNER_SETTING = Setting.simpleString("cloud.aws.signer", Property.NodeScope, Property.Shared);
    /**
     * cloud.aws.region: Region. Shared with discovery-ec2 plugin
     */
    Setting<String> REGION_SETTING =
        new Setting<>("cloud.aws.region", "", s -> s.toLowerCase(Locale.ROOT), Property.NodeScope, Property.Shared);

    /**
     * Defines specific s3 settings starting with cloud.aws.s3.
     */
    interface CLOUD_S3 {
        /**
         * cloud.aws.s3.access_key: AWS Access key specific for S3 API calls. Defaults to cloud.aws.access_key.
         * @see AwsS3Service#KEY_SETTING
         */
        Setting<String> KEY_SETTING =
            new Setting<>("cloud.aws.s3.access_key", AwsS3Service.KEY_SETTING, Function.identity(),
                Property.NodeScope, Property.Filtered);
        /**
         * cloud.aws.s3.secret_key: AWS Secret key specific for S3 API calls. Defaults to cloud.aws.secret_key.
         * @see AwsS3Service#SECRET_SETTING
         */
        Setting<String> SECRET_SETTING =
            new Setting<>("cloud.aws.s3.secret_key", AwsS3Service.SECRET_SETTING, Function.identity(),
                Property.NodeScope, Property.Filtered);
        /**
         * cloud.aws.s3.protocol: Protocol for AWS API specific for S3 API calls: http or https. Defaults to cloud.aws.protocol.
         * @see AwsS3Service#PROTOCOL_SETTING
         */
        Setting<Protocol> PROTOCOL_SETTING =
            new Setting<>("cloud.aws.s3.protocol", AwsS3Service.PROTOCOL_SETTING, s -> Protocol.valueOf(s.toUpperCase(Locale.ROOT)),
                Property.NodeScope);
        /**
         * cloud.aws.s3.proxy.host: In case of proxy, define its hostname/IP specific for S3 API calls. Defaults to cloud.aws.proxy.host.
         * @see AwsS3Service#PROXY_HOST_SETTING
         */
        Setting<String> PROXY_HOST_SETTING =
            new Setting<>("cloud.aws.s3.proxy.host", AwsS3Service.PROXY_HOST_SETTING, Function.identity(),
                Property.NodeScope);
        /**
         * cloud.aws.s3.proxy.port: In case of proxy, define its port specific for S3 API calls.  Defaults to cloud.aws.proxy.port.
         * @see AwsS3Service#PROXY_PORT_SETTING
         */
        Setting<Integer> PROXY_PORT_SETTING =
            new Setting<>("cloud.aws.s3.proxy.port", AwsS3Service.PROXY_PORT_SETTING,
                s -> Setting.parseInt(s, 0, 1<<16, "cloud.aws.s3.proxy.port"), Property.NodeScope);
        /**
         * cloud.aws.s3.proxy.username: In case of proxy with auth, define the username specific for S3 API calls.
         * Defaults to cloud.aws.proxy.username.
         * @see AwsS3Service#PROXY_USERNAME_SETTING
         */
        Setting<String> PROXY_USERNAME_SETTING =
            new Setting<>("cloud.aws.s3.proxy.username", AwsS3Service.PROXY_USERNAME_SETTING, Function.identity(),
                Property.NodeScope);
        /**
         * cloud.aws.s3.proxy.password: In case of proxy with auth, define the password specific for S3 API calls.
         * Defaults to cloud.aws.proxy.password.
         * @see AwsS3Service#PROXY_PASSWORD_SETTING
         */
        Setting<String> PROXY_PASSWORD_SETTING =
            new Setting<>("cloud.aws.s3.proxy.password", AwsS3Service.PROXY_PASSWORD_SETTING, Function.identity(),
                Property.NodeScope, Property.Filtered);
        /**
         * cloud.aws.s3.signer: If you are using an old AWS API version, you can define a Signer. Specific for S3 API calls.
         * Defaults to cloud.aws.signer.
         * @see AwsS3Service#SIGNER_SETTING
         */
        Setting<String> SIGNER_SETTING =
            new Setting<>("cloud.aws.s3.signer", AwsS3Service.SIGNER_SETTING, Function.identity(), Property.NodeScope);
        /**
         * cloud.aws.s3.region: Region specific for S3 API calls. Defaults to cloud.aws.region.
         * @see AwsS3Service#REGION_SETTING
         */
        Setting<String> REGION_SETTING =
            new Setting<>("cloud.aws.s3.region", AwsS3Service.REGION_SETTING, s -> s.toLowerCase(Locale.ROOT),
                Property.NodeScope);
        /**
         * cloud.aws.s3.endpoint: Endpoint. If not set, endpoint will be guessed based on region setting.
         */
        Setting<String> ENDPOINT_SETTING = Setting.simpleString("cloud.aws.s3.endpoint", Property.NodeScope);
    }

    AmazonS3 client(String endpoint, Protocol protocol, String region, String account, String key, Integer maxRetries,
                    boolean useThrottleRetries);
}
