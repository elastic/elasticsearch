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

package org.elasticsearch.discovery.ec2;

import com.amazonaws.util.json.Jackson;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zen.UnicastHostsProvider;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.TransportService;

import java.io.UncheckedIOException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Closeable;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class Ec2DiscoveryPlugin extends Plugin implements DiscoveryPlugin, Closeable {

    private static Logger logger = Loggers.getLogger(Ec2DiscoveryPlugin.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);

    public static final String EC2 = "ec2";

    static {
        SpecialPermission.check();
        // Initializing Jackson requires RuntimePermission accessDeclaredMembers
        // The ClientConfiguration class requires RuntimePermission getClassLoader
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            try {
                // kick jackson to do some static caching of declared members info
                Jackson.jsonNodeOf("{}");
                // ClientConfiguration clinit has some classloader problems
                // TODO: fix that
                Class.forName("com.amazonaws.ClientConfiguration");
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    private Settings settings;
    // stashed when created in order to properly close
    private final SetOnce<AwsEc2ServiceImpl> ec2Service = new SetOnce<>();

    public Ec2DiscoveryPlugin(Settings settings) {
        this.settings = settings;
    }



    @Override
    public NetworkService.CustomNameResolver getCustomNameResolver(Settings settings) {
        logger.debug("Register _ec2_, _ec2:xxx_ network names");
        return new Ec2NameResolver(settings);
    }

    @Override
    public Map<String, Supplier<UnicastHostsProvider>> getZenHostsProviders(TransportService transportService,
                                                                            NetworkService networkService) {
        return Collections.singletonMap(EC2, () -> {
            ec2Service.set(new AwsEc2ServiceImpl(settings));
            return new AwsEc2UnicastHostsProvider(settings, transportService, ec2Service.get());
        });
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
        // Register EC2 discovery settings: discovery.ec2
        AwsEc2Service.ACCESS_KEY_SETTING,
        AwsEc2Service.SECRET_KEY_SETTING,
        AwsEc2Service.ENDPOINT_SETTING,
        AwsEc2Service.PROTOCOL_SETTING,
        AwsEc2Service.PROXY_HOST_SETTING,
        AwsEc2Service.PROXY_PORT_SETTING,
        AwsEc2Service.PROXY_USERNAME_SETTING,
        AwsEc2Service.PROXY_PASSWORD_SETTING,
        AwsEc2Service.READ_TIMEOUT_SETTING,
        AwsEc2Service.HOST_TYPE_SETTING,
        AwsEc2Service.ANY_GROUP_SETTING,
        AwsEc2Service.GROUPS_SETTING,
        AwsEc2Service.AVAILABILITY_ZONES_SETTING,
        AwsEc2Service.NODE_CACHE_TIME_SETTING,
        AwsEc2Service.TAG_SETTING,
        // Register cloud node settings: cloud.node
        AwsEc2Service.AUTO_ATTRIBUTE_SETTING);
    }

    @Override
    public Settings additionalSettings() {
        Settings.Builder builder = Settings.builder();

        // Adds a node attribute for the ec2 availability zone
        String azMetadataUrl = AwsEc2ServiceImpl.EC2_METADATA_URL + "placement/availability-zone";
        builder.put(getAvailabilityZoneNodeAttributes(settings, azMetadataUrl));
        return builder.build();
    }

    // pkg private for testing
    @SuppressForbidden(reason = "We call getInputStream in doPrivileged and provide SocketPermission")
    static Settings getAvailabilityZoneNodeAttributes(Settings settings, String azMetadataUrl) {
        if (AwsEc2Service.AUTO_ATTRIBUTE_SETTING.get(settings) == false) {
            return Settings.EMPTY;
        }
        Settings.Builder attrs = Settings.builder();

        final URL url;
        final URLConnection urlConnection;
        try {
            url = new URL(azMetadataUrl);
            logger.debug("obtaining ec2 [placement/availability-zone] from ec2 meta-data url {}", url);
            urlConnection = SocketAccess.doPrivilegedIOException(url::openConnection);
            urlConnection.setConnectTimeout(2000);
        } catch (IOException e) {
            // should not happen, we know the url is not malformed, and openConnection does not actually hit network
            throw new UncheckedIOException(e);
        }

        try (InputStream in = SocketAccess.doPrivilegedIOException(urlConnection::getInputStream);
             BufferedReader urlReader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {

            String metadataResult = urlReader.readLine();
            if (metadataResult == null || metadataResult.length() == 0) {
                throw new IllegalStateException("no ec2 metadata returned from " + url);
            } else {
                attrs.put(Node.NODE_ATTRIBUTES.getKey() + "aws_availability_zone", metadataResult);
            }
        } catch (IOException e) {
            // this is lenient so the plugin does not fail when installed outside of ec2
            logger.error("failed to get metadata for [placement/availability-zone]", e);
        }

        return attrs.build();
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(ec2Service.get());
    }
}
