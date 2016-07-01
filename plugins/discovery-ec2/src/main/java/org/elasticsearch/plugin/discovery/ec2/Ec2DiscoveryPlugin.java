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

package org.elasticsearch.plugin.discovery.ec2;

import org.elasticsearch.SpecialPermission;
import org.elasticsearch.cloud.aws.AwsEc2Service;
import org.elasticsearch.cloud.aws.AwsEc2ServiceImpl;
import org.elasticsearch.cloud.aws.Ec2Module;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.ec2.AwsEc2UnicastHostsProvider;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.plugins.Plugin;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 *
 */
public class Ec2DiscoveryPlugin extends Plugin {

    public static final String EC2 = "ec2";

    // ClientConfiguration clinit has some classloader problems
    // TODO: fix that
    static {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            @Override
            public Void run() {
                try {
                    Class.forName("com.amazonaws.ClientConfiguration");
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
                return null;
            }
        });
    }

    @Override
    public Collection<Module> nodeModules() {
        Collection<Module> modules = new ArrayList<>();
        modules.add(new Ec2Module());
        return modules;
    }

    @Override
    @SuppressWarnings("rawtypes") // Supertype uses rawtype
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        Collection<Class<? extends LifecycleComponent>> services = new ArrayList<>();
        services.add(AwsEc2ServiceImpl.class);
        return services;
    }

    public void onModule(DiscoveryModule discoveryModule) {
        discoveryModule.addDiscoveryType(EC2, ZenDiscovery.class);
        discoveryModule.addUnicastHostProvider(EC2, AwsEc2UnicastHostsProvider.class);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
        // Register global cloud aws settings: cloud.aws (might have been registered in ec2 plugin)
        AwsEc2Service.KEY_SETTING,
        AwsEc2Service.SECRET_SETTING,
        AwsEc2Service.PROTOCOL_SETTING,
        AwsEc2Service.PROXY_HOST_SETTING,
        AwsEc2Service.PROXY_PORT_SETTING,
        AwsEc2Service.PROXY_USERNAME_SETTING,
        AwsEc2Service.PROXY_PASSWORD_SETTING,
        AwsEc2Service.SIGNER_SETTING,
        AwsEc2Service.REGION_SETTING,
        // Register EC2 specific settings: cloud.aws.ec2
        AwsEc2Service.CLOUD_EC2.KEY_SETTING,
        AwsEc2Service.CLOUD_EC2.SECRET_SETTING,
        AwsEc2Service.CLOUD_EC2.PROTOCOL_SETTING,
        AwsEc2Service.CLOUD_EC2.PROXY_HOST_SETTING,
        AwsEc2Service.CLOUD_EC2.PROXY_PORT_SETTING,
        AwsEc2Service.CLOUD_EC2.PROXY_USERNAME_SETTING,
        AwsEc2Service.CLOUD_EC2.PROXY_PASSWORD_SETTING,
        AwsEc2Service.CLOUD_EC2.SIGNER_SETTING,
        AwsEc2Service.CLOUD_EC2.REGION_SETTING,
        AwsEc2Service.CLOUD_EC2.ENDPOINT_SETTING,
        // Register EC2 discovery settings: discovery.ec2
        AwsEc2Service.DISCOVERY_EC2.HOST_TYPE_SETTING,
        AwsEc2Service.DISCOVERY_EC2.ANY_GROUP_SETTING,
        AwsEc2Service.DISCOVERY_EC2.GROUPS_SETTING,
        AwsEc2Service.DISCOVERY_EC2.AVAILABILITY_ZONES_SETTING,
        AwsEc2Service.DISCOVERY_EC2.NODE_CACHE_TIME_SETTING,
        AwsEc2Service.DISCOVERY_EC2.TAG_SETTING,
        // Register cloud node settings: cloud.node
        AwsEc2Service.AUTO_ATTRIBUTE_SETTING);
    }
}
