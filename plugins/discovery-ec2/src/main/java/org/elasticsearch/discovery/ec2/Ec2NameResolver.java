/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery.ec2;

import com.amazonaws.util.EC2MetadataUtils;

import org.elasticsearch.common.network.NetworkService.CustomNameResolver;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;

import static org.elasticsearch.discovery.ec2.AwsEc2Utils.X_AWS_EC_2_METADATA_TOKEN;

/**
 * Resolves certain ec2 related 'meta' hostnames into an actual hostname
 * obtained from ec2 meta-data.
 * <p>
 * Valid config values for {@link Ec2HostnameType}s are -
 * <ul>
 * <li>_ec2_ - maps to privateIpv4</li>
 * <li>_ec2:privateIp_ - maps to privateIpv4</li>
 * <li>_ec2:privateIpv4_</li>
 * <li>_ec2:privateDns_</li>
 * <li>_ec2:publicIp_ - maps to publicIpv4</li>
 * <li>_ec2:publicIpv4_</li>
 * <li>_ec2:publicDns_</li>
 * </ul>
 *
 * @author Paul_Loy (keteracel)
 */
class Ec2NameResolver implements CustomNameResolver {

    private static final Logger logger = LogManager.getLogger(Ec2NameResolver.class);

    /**
     * enum that can be added to over time with more meta-data types (such as ipv6 when this is available)
     *
     * @author Paul_Loy
     */
    private enum Ec2HostnameType {

        PRIVATE_IPv4("ec2:privateIpv4", "local-ipv4"),
        PRIVATE_DNS("ec2:privateDns", "local-hostname"),
        PUBLIC_IPv4("ec2:publicIpv4", "public-ipv4"),
        PUBLIC_DNS("ec2:publicDns", "public-hostname"),

        // some less verbose defaults
        PUBLIC_IP("ec2:publicIp", PUBLIC_IPv4.ec2Name),
        PRIVATE_IP("ec2:privateIp", PRIVATE_IPv4.ec2Name),
        EC2("ec2", PRIVATE_IPv4.ec2Name);

        final String configName;
        final String ec2Name;

        Ec2HostnameType(String configName, String ec2Name) {
            this.configName = configName;
            this.ec2Name = ec2Name;
        }
    }

    /**
     * @param type the ec2 hostname type to discover.
     * @return the appropriate host resolved from ec2 meta-data, or null if it cannot be obtained.
     * @see CustomNameResolver#resolveIfPossible(String)
     */
    @SuppressForbidden(reason = "We call getInputStream in doPrivileged and provide SocketPermission")
    public InetAddress[] resolve(Ec2HostnameType type) throws IOException {
        InputStream in = null;
        String metadataUrl = EC2MetadataUtils.getHostAddressForEC2MetadataService() + "/latest/meta-data/" + type.ec2Name;
        String metadataTokenUrl = EC2MetadataUtils.getHostAddressForEC2MetadataService() + "/latest/api/token";
        try {
            URL url = new URL(metadataUrl);
            logger.debug("obtaining ec2 hostname from ec2 meta-data url {}", url);
            URLConnection urlConnection = SocketAccess.doPrivilegedIOException(url::openConnection);
            urlConnection.setConnectTimeout(2000);
            AwsEc2Utils.getMetadataToken(metadataTokenUrl)
                .ifPresent(token -> urlConnection.setRequestProperty(X_AWS_EC_2_METADATA_TOKEN, token));

            in = SocketAccess.doPrivilegedIOException(urlConnection::getInputStream);
            BufferedReader urlReader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));

            String metadataResult = urlReader.readLine();
            if (metadataResult == null || metadataResult.length() == 0) {
                throw new IOException("no gce metadata returned from [" + url + "] for [" + type.configName + "]");
            }
            // only one address: because we explicitly ask for only one via the Ec2HostnameType
            return new InetAddress[] { InetAddress.getByName(metadataResult) };
        } catch (IOException e) {
            throw new IOException("IOException caught when fetching InetAddress from [" + metadataUrl + "]", e);
        } finally {
            IOUtils.closeWhileHandlingException(in);
        }
    }

    @Override
    public InetAddress[] resolveDefault() {
        return null; // using this, one has to explicitly specify _ec2_ in network setting
        // return resolve(Ec2HostnameType.DEFAULT, false);
    }

    @Override
    public InetAddress[] resolveIfPossible(String value) throws IOException {
        for (Ec2HostnameType type : Ec2HostnameType.values()) {
            if (type.configName.equals(value)) {
                return resolve(type);
            }
        }
        return null;
    }

}
