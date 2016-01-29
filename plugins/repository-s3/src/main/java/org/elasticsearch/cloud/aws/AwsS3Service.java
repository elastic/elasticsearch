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

import java.util.Locale;
import java.util.function.Function;

/**
 *
 */
public interface AwsS3Service extends LifecycleComponent<AwsS3Service> {

    // Global AWS settings (shared between discovery-ec2 and repository-s3)
    // Each setting starting with `cloud.aws` also exists in discovery-ec2 project. Don't forget to update
    // the code there if you change anything here.
    Setting<String> KEY_SETTING = Setting.simpleString("cloud.aws.access_key", false, Setting.Scope.CLUSTER);
    Setting<String> SECRET_SETTING = Setting.simpleString("cloud.aws.secret_key", false, Setting.Scope.CLUSTER);
    Setting<Protocol> PROTOCOL_SETTING =
        new Setting<>("cloud.aws.protocol", "https", s -> Protocol.valueOf(s.toUpperCase(Locale.ROOT)), false, Setting.Scope.CLUSTER);
    Setting<String> PROXY_HOST_SETTING = Setting.simpleString("cloud.aws.proxy.host", false, Setting.Scope.CLUSTER);
    Setting<Integer> PROXY_PORT_SETTING = Setting.intSetting("cloud.aws.proxy.port", 80, 0, 1<<16, false, Setting.Scope.CLUSTER);
    Setting<String> PROXY_USERNAME_SETTING = Setting.simpleString("cloud.aws.proxy.username", false, Setting.Scope.CLUSTER);
    Setting<String> PROXY_PASSWORD_SETTING = Setting.simpleString("cloud.aws.proxy.password", false, Setting.Scope.CLUSTER);
    Setting<String> SIGNER_SETTING = Setting.simpleString("cloud.aws.signer", false, Setting.Scope.CLUSTER);
    Setting<String> REGION_SETTING = new Setting<>("cloud.aws.region", "", s -> s.toLowerCase(Locale.ROOT), false, Setting.Scope.CLUSTER);

    // Specific S3 settings
    interface CLOUD_S3 {
        Setting<String> KEY_SETTING =
            new Setting<>("cloud.aws.s3.access_key", AwsS3Service.KEY_SETTING, Function.identity(), false, Setting.Scope.CLUSTER);
        Setting<String> SECRET_SETTING =
            new Setting<>("cloud.aws.s3.secret_key", AwsS3Service.SECRET_SETTING, Function.identity(), false, Setting.Scope.CLUSTER);
        Setting<Protocol> PROTOCOL_SETTING =
            new Setting<>("cloud.aws.s3.protocol", AwsS3Service.PROTOCOL_SETTING, s -> Protocol.valueOf(s.toUpperCase(Locale.ROOT)), false,
                Setting.Scope.CLUSTER);
        Setting<String> PROXY_HOST_SETTING =
            new Setting<>("cloud.aws.s3.proxy.host", AwsS3Service.PROXY_HOST_SETTING, Function.identity(), false, Setting.Scope.CLUSTER);
        Setting<Integer> PROXY_PORT_SETTING =
            new Setting<>("cloud.aws.s3.proxy.port", AwsS3Service.PROXY_PORT_SETTING,
                s -> Setting.parseInt(s, 0, 1<<16, "cloud.aws.s3.proxy.port"), false, Setting.Scope.CLUSTER);
        Setting<String> PROXY_USERNAME_SETTING =
            new Setting<>("cloud.aws.s3.proxy.username", AwsS3Service.PROXY_USERNAME_SETTING, Function.identity(), false,
                Setting.Scope.CLUSTER);
        Setting<String> PROXY_PASSWORD_SETTING =
            new Setting<>("cloud.aws.s3.proxy.password", AwsS3Service.PROXY_PASSWORD_SETTING, Function.identity(), false,
                Setting.Scope.CLUSTER);
        Setting<String> SIGNER_SETTING =
            new Setting<>("cloud.aws.s3.signer", AwsS3Service.SIGNER_SETTING, Function.identity(), false, Setting.Scope.CLUSTER);
        Setting<String> REGION_SETTING =
            new Setting<>("cloud.aws.s3.region", AwsS3Service.REGION_SETTING, s -> s.toLowerCase(Locale.ROOT), false,
                Setting.Scope.CLUSTER);
        Setting<String> ENDPOINT_SETTING =
            Setting.simpleString("cloud.aws.s3.endpoint", false, Setting.Scope.CLUSTER);
    }

    AmazonS3 client(String endpoint, Protocol protocol, String region, String account, String key, Integer maxRetries);
}
