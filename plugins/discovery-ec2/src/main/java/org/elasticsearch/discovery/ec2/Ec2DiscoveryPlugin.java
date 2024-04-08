/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery.ec2;

import com.amazonaws.util.EC2MetadataUtils;
import com.amazonaws.util.json.Jackson;

import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.discovery.SeedHostsProvider;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.transport.TransportService;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
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

import static org.elasticsearch.discovery.ec2.AwsEc2Utils.X_AWS_EC_2_METADATA_TOKEN;

public class Ec2DiscoveryPlugin extends Plugin implements DiscoveryPlugin, ReloadablePlugin {

    private static final Logger logger = LogManager.getLogger(Ec2DiscoveryPlugin.class);
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
            } catch (final ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    private final Settings settings;
    // protected for testing
    protected final AwsEc2Service ec2Service;

    public Ec2DiscoveryPlugin(Settings settings) {
        this(settings, new AwsEc2ServiceImpl());
    }

    @SuppressWarnings("this-escape")
    protected Ec2DiscoveryPlugin(Settings settings, AwsEc2ServiceImpl ec2Service) {
        this.settings = settings;
        this.ec2Service = ec2Service;
        // eagerly load client settings when secure settings are accessible
        reload(settings);
    }

    @Override
    public NetworkService.CustomNameResolver getCustomNameResolver(Settings _settings) {
        logger.debug("Register _ec2_, _ec2:xxx_ network names");
        return new Ec2NameResolver();
    }

    @Override
    public Map<String, Supplier<SeedHostsProvider>> getSeedHostProviders(TransportService transportService, NetworkService networkService) {
        return Collections.singletonMap(EC2, () -> new AwsEc2SeedHostsProvider(settings, transportService, ec2Service));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            // Register EC2 discovery settings: discovery.ec2
            Ec2ClientSettings.ACCESS_KEY_SETTING,
            Ec2ClientSettings.SECRET_KEY_SETTING,
            Ec2ClientSettings.SESSION_TOKEN_SETTING,
            Ec2ClientSettings.ENDPOINT_SETTING,
            Ec2ClientSettings.PROTOCOL_SETTING,
            Ec2ClientSettings.PROXY_HOST_SETTING,
            Ec2ClientSettings.PROXY_PORT_SETTING,
            Ec2ClientSettings.PROXY_SCHEME_SETTING,
            Ec2ClientSettings.PROXY_USERNAME_SETTING,
            Ec2ClientSettings.PROXY_PASSWORD_SETTING,
            Ec2ClientSettings.READ_TIMEOUT_SETTING,
            AwsEc2Service.HOST_TYPE_SETTING,
            AwsEc2Service.ANY_GROUP_SETTING,
            AwsEc2Service.GROUPS_SETTING,
            AwsEc2Service.AVAILABILITY_ZONES_SETTING,
            AwsEc2Service.NODE_CACHE_TIME_SETTING,
            AwsEc2Service.TAG_SETTING,
            // Register cloud node settings: cloud.node
            AwsEc2Service.AUTO_ATTRIBUTE_SETTING
        );
    }

    @Override
    public Settings additionalSettings() {
        final Settings.Builder builder = Settings.builder();

        // Adds a node attribute for the ec2 availability zone
        final String azMetadataUrl = EC2MetadataUtils.getHostAddressForEC2MetadataService()
            + "/latest/meta-data/placement/availability-zone";
        String azMetadataTokenUrl = EC2MetadataUtils.getHostAddressForEC2MetadataService() + "/latest/api/token";
        builder.put(getAvailabilityZoneNodeAttributes(settings, azMetadataUrl, azMetadataTokenUrl));
        return builder.build();
    }

    // pkg private for testing
    @SuppressForbidden(reason = "We call getInputStream in doPrivileged and provide SocketPermission")
    static Settings getAvailabilityZoneNodeAttributes(Settings settings, String azMetadataUrl, String azMetadataTokenUrl) {
        if (AwsEc2Service.AUTO_ATTRIBUTE_SETTING.get(settings) == false) {
            return Settings.EMPTY;
        }
        final Settings.Builder attrs = Settings.builder();

        final URL url;
        final URLConnection urlConnection;
        try {
            url = new URL(azMetadataUrl);
            logger.debug("obtaining ec2 [placement/availability-zone] from ec2 meta-data url {}", url);
            urlConnection = SocketAccess.doPrivilegedIOException(url::openConnection);
            urlConnection.setConnectTimeout(2000);
            AwsEc2Utils.getMetadataToken(azMetadataTokenUrl)
                .ifPresent(token -> urlConnection.setRequestProperty(X_AWS_EC_2_METADATA_TOKEN, token));
        } catch (final IOException e) {
            // should not happen, we know the url is not malformed, and openConnection does not actually hit network
            throw new UncheckedIOException(e);
        }

        try (
            InputStream in = SocketAccess.doPrivilegedIOException(urlConnection::getInputStream);
            BufferedReader urlReader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))
        ) {

            final String metadataResult = urlReader.readLine();
            if ((metadataResult == null) || (metadataResult.length() == 0)) {
                throw new IllegalStateException("no ec2 metadata returned from " + url);
            } else {
                attrs.put(Node.NODE_ATTRIBUTES.getKey() + "aws_availability_zone", metadataResult);
            }
        } catch (final IOException e) {
            // this is lenient so the plugin does not fail when installed outside of ec2
            logger.error("failed to get metadata for [placement/availability-zone]", e);
        }

        return attrs.build();
    }

    @Override
    public void close() throws IOException {
        ec2Service.close();
    }

    @Override
    public void reload(Settings settingsToLoad) {
        // secure settings should be readable
        final Ec2ClientSettings clientSettings = Ec2ClientSettings.getClientSettings(settingsToLoad);
        ec2Service.refreshAndClearCache(clientSettings);
    }
}
