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

package org.elasticsearch.plugin.cloud.aws;

import org.elasticsearch.cloud.aws.AwsEc2ServiceImpl;
import org.elasticsearch.cloud.aws.AwsModule;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.ec2.AwsEc2UnicastHostsProvider;
import org.elasticsearch.discovery.ec2.Ec2Discovery;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardRepository;
import org.elasticsearch.repositories.RepositoriesModule;
import org.elasticsearch.repositories.s3.S3Repository;

import java.util.ArrayList;
import java.util.Collection;

/**
 *
 */
public class CloudAwsPlugin extends Plugin {

    private final Settings settings;

    public CloudAwsPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public String name() {
        return "cloud-aws";
    }

    @Override
    public String description() {
        return "Cloud AWS Plugin";
    }

    @Override
    public Collection<Module> nodeModules() {
        Collection<Module> modules = new ArrayList<>();
        if (settings.getAsBoolean("cloud.enabled", true)) {
            modules.add(new AwsModule());
        }
        return modules;
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        Collection<Class<? extends LifecycleComponent>> services = new ArrayList<>();
        if (settings.getAsBoolean("cloud.enabled", true)) {
            services.add(AwsModule.getS3ServiceImpl());
            services.add(AwsEc2ServiceImpl.class);
        }
        return services;
    }

    public void onModule(RepositoriesModule repositoriesModule) {
        if (settings.getAsBoolean("cloud.enabled", true)) {
            repositoriesModule.registerRepository(S3Repository.TYPE, S3Repository.class, BlobStoreIndexShardRepository.class);
        }
    }

    public void onModule(DiscoveryModule discoveryModule) {
        discoveryModule.addDiscoveryType("ec2", Ec2Discovery.class);
        discoveryModule.addUnicastHostProvider(AwsEc2UnicastHostsProvider.class);
    }
}
