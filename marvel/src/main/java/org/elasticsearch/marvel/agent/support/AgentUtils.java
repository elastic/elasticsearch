/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.support;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.*;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AgentUtils {

    public static XContentBuilder nodeToXContent(DiscoveryNode node, XContentBuilder builder) throws IOException {
        return nodeToXContent(node, null, builder);
    }

    public static XContentBuilder nodeToXContent(DiscoveryNode node, Boolean isMasterNode, XContentBuilder builder) throws IOException {
        builder.field("id", node.id());
        builder.field("name", node.name());
        builder.field("transport_address", node.address());

        if (node.address().uniqueAddressTypeId() == 1) { // InetSocket
            InetSocketTransportAddress address = (InetSocketTransportAddress) node.address();
            InetSocketAddress inetSocketAddress = address.address();
            InetAddress inetAddress = inetSocketAddress.getAddress();
            if (inetAddress != null) {
                builder.field("ip", inetAddress.getHostAddress());
                builder.field("host", inetAddress.getHostName());
                builder.field("ip_port", inetAddress.getHostAddress() + ":" + inetSocketAddress.getPort());
            }
        } else if (node.address().uniqueAddressTypeId() == 2) {  // local transport
            builder.field("ip_port", "_" + node.address()); // will end up being "_local[ID]"
        }

        builder.field("master_node", node.isMasterNode());
        builder.field("data_node", node.isDataNode());
        if (isMasterNode != null) {
            builder.field("master", isMasterNode.booleanValue());
        }

        if (!node.attributes().isEmpty()) {
            builder.startObject("attributes");
            for (Map.Entry<String, String> attr : node.attributes().entrySet()) {
                builder.field(attr.getKey(), attr.getValue());
            }
            builder.endObject();
        }
        return builder;
    }

    public static String nodeDescription(DiscoveryNode node) {
        StringBuilder builder = new StringBuilder().append("[").append(node.name()).append("]");
        if (node.address().uniqueAddressTypeId() == 1) { // InetSocket
            InetSocketTransportAddress address = (InetSocketTransportAddress) node.address();
            InetSocketAddress inetSocketAddress = address.address();
            InetAddress inetAddress = inetSocketAddress.getAddress();
            if (inetAddress != null) {
                builder.append("[").append(inetAddress.getHostAddress()).append(":").append(inetSocketAddress.getPort()).append("]");
            }
        }
        return builder.toString();
    }

    public static String[] extractHostsFromAddress(BoundTransportAddress boundAddress, ESLogger logger) {
        if (boundAddress == null || boundAddress.boundAddress() == null) {
            logger.debug("local http server is not yet started. can't connect");
            return null;
        }

        if (boundAddress.boundAddress().uniqueAddressTypeId() != 1) {
            logger.error("local node is not bound via the http transport. can't connect");
            return null;
        }
        InetSocketTransportAddress address = (InetSocketTransportAddress) boundAddress.boundAddress();
        InetSocketAddress inetSocketAddress = address.address();
        InetAddress inetAddress = inetSocketAddress.getAddress();
        if (inetAddress == null) {
            logger.error("failed to extract the ip address of current node.");
            return null;
        }

        String host = inetAddress.getHostAddress();
        if (host.indexOf(":") >= 0) {
            // ipv6
            host = "[" + host + "]";
        }

        return new String[]{host + ":" + inetSocketAddress.getPort()};
    }

    public static URL parseHostWithPath(String host, String path) throws URISyntaxException, MalformedURLException {

        if (!host.contains("://")) {
            // prefix with http
            host = "http://" + host;
        }
        if (!host.endsWith("/")) {
            // make sure we can safely resolves sub paths and not replace parent folders
            host = host + "/";
        }

        URL hostUrl = new URL(host);

        if (hostUrl.getPort() == -1) {
            // url has no port, default to 9200 - sadly we need to rebuild..
            StringBuilder newUrl = new StringBuilder(hostUrl.getProtocol() + "://");
            if (hostUrl.getUserInfo() != null) {
                newUrl.append(hostUrl.getUserInfo()).append("@");
            }
            newUrl.append(hostUrl.getHost()).append(":9200").append(hostUrl.toURI().getPath());

            hostUrl = new URL(newUrl.toString());

        }
        return new URL(hostUrl, path);

    }

    public static int parseIndexVersionFromTemplate(byte[] template) throws UnsupportedEncodingException {
        Pattern versionRegex = Pattern.compile("marvel.index_format\"\\s*:\\s*\"?(\\d+)\"?");
        Matcher matcher = versionRegex.matcher(new String(template, "UTF-8"));
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        } else {
            return -1;
        }
    }

    private static final String userInfoChars = "\\w-\\._~!$&\\'\\(\\)*+,;=%";
    private static Pattern urlPwdSanitizer = Pattern.compile("([" + userInfoChars + "]+?):[" + userInfoChars + "]+?@");

    public static String santizeUrlPwds(Object text) {
        if (text == null) {
            return null;
        }
        Matcher matcher = urlPwdSanitizer.matcher(text.toString());
        return matcher.replaceAll("$1:XXXXXX@");
    }
}
