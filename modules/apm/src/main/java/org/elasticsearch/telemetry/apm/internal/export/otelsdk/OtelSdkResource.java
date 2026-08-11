/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.resources.ResourceBuilder;

import org.elasticsearch.Build;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.monitor.os.OsProbe;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Builds the OpenTelemetry {@link Resource} attached to every metric, span and log this node exports.
 *
 * <p>It reproduces the host, OS, process, Kubernetes, container and environment attributes,
 * so APM Server maps them to the same ECS fields. Operators can add or override attributes via the
 * {@code telemetry.resource.*} setting ({@link OtelSdkSettings#TELEMETRY_RESOURCE_ATTRIBUTES}).
 */
final class OtelSdkResource {

    private static final Pattern CONTAINER_ID_64 = Pattern.compile("^[0-9a-fA-F]{64}$");
    private static final Pattern SHORTENED_UUID = Pattern.compile(
        "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4,}"
    );
    private static final Pattern AWS_FARGATE = Pattern.compile("^[0-9a-fA-F]{32}-[0-9]{10}$");
    private static final Pattern POD_PATH = Pattern.compile("(?:^/kubepods[\\S]*/pod([^/]+)$)|(?:kubepods[^/]*-pod([^/]+)\\.slice)");
    private static final Pattern CGROUP_V2_CONTAINER = Pattern.compile("^.*([0-9a-fA-F]{64}).*$");

    private OtelSdkResource() {}

    static Resource get(Settings settings) {
        return get(settings, System::getenv);
    }

    static Resource get(Settings settings, UnaryOperator<String> getenv) {
        ResourceBuilder builder = Resource.builder()
            .put("service.name", "self-managed-elasticsearch") // other deployment types should override via
                                                               // telemetry.resource.service.name
            .put("service.version", Build.current().version())
            .put("process.runtime.name", "Java")
            .put("process.runtime.version", System.getProperty("java.version"))
            .put("telemetry.distro.name", "elasticsearch-otel-sdk")
            .put("telemetry.distro.version", Build.current().version())
            .put("host.arch", System.getProperty("os.arch"))
            .put("os.type", System.getProperty("os.name"))
            .put("process.pid", ProcessHandle.current().pid())
            // TODO change the settings emitted by the controller so it doesn't mention agent.
            .put("deployment.environment", settings.get("telemetry.agent.environment", "dev"));

        String nodeName = settings.get("node.name");
        if (nodeName != null) {
            builder.put("service.instance.id", nodeName);
        }

        putIfPresent(builder, "k8s.namespace.name", getenv.apply("KUBERNETES_NAMESPACE"));
        putIfPresent(builder, "k8s.node.name", getenv.apply("KUBERNETES_NODE_NAME"));
        putIfPresent(builder, "k8s.pod.name", getenv.apply("KUBERNETES_POD_NAME"));
        putIfPresent(builder, "k8s.pod.uid", getenv.apply("KUBERNETES_POD_UID"));
        putIfPresent(builder, "host.name", getenv.apply("HOSTNAME"));
        putIfPresent(builder, "container.id", containerId());
        putIfPresent(builder, "process.executable.path", ProcessHandle.current().info().command().orElse(null));

        List<String> hostIps = hostIps();
        if (hostIps.isEmpty() == false) {
            builder.put(AttributeKey.stringArrayKey("host.ip"), hostIps);
        }

        OtelSdkSettings.TELEMETRY_RESOURCE_ATTRIBUTES.getAsMap(settings).forEach(builder::put);
        return Resource.getDefault().merge(builder.build());
    }

    private static void putIfPresent(ResourceBuilder builder, String key, String value) {
        if (value != null && value.isEmpty() == false) {
            builder.put(key, value);
        }
    }

    /**
     * Collects this node's non-loopback, non-link-local IP addresses for the {@code host.ip} attribute.
     * APM Server does not do that for OTLP intake, so we emit the addresses here to keep parity.
     */
    private static List<String> hostIps() {
        List<String> ips = new ArrayList<>();
        try {
            for (NetworkInterface ni : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                for (InetAddress addr : Collections.list(ni.getInetAddresses())) {
                    if (addr.isLoopbackAddress() == false && addr.isLinkLocalAddress() == false && addr.isAnyLocalAddress() == false) {
                        ips.add(NetworkAddress.format(addr));
                    }
                }
            }
        } catch (SocketException e) {
            return List.of();
        }
        return ips;
    }

    private static String containerId() {
        var cgroup = OsProbe.getInstance().osStats().getCgroup();
        String id = cgroup == null ? null : parseContainerId(cgroup.getCpuAcctControlGroup());
        return id != null ? id : mountInfoContainerId();
    }

    /**
     * Mirroring fallback logic for containerId from APM Agent
     * @see <a href=
     *      "https://github.com/elastic/apm-agent-java/blob/7961a5c7c4fc1fb28de8e41e40bb30f32da384f7/apm-agent-core/src/main/java/co/elastic/apm/agent/impl/metadata/SystemInfo.java">
     *      SystemInfo#parseCgroupsV2ContainerId</a>
     */
    @SuppressForbidden(reason = "access /proc/self/mountinfo")
    private static String mountInfoContainerId() {
        Path mountInfo = PathUtils.get("/proc/self/mountinfo");
        if (Files.isRegularFile(mountInfo) == false) {
            return null;
        }
        try {
            return parseMountInfoContainerId(Files.readAllLines(mountInfo, StandardCharsets.UTF_8));
        } catch (IOException e) {
            return null;
        }
    }

    static String parseMountInfoContainerId(List<String> mountInfoLines) {
        for (String line : mountInfoLines) {
            if (line.indexOf("/etc/hostname") > 0) {
                String[] fields = line.split(" ");
                if (fields.length > 3) {
                    Matcher matcher = CGROUP_V2_CONTAINER.matcher(fields[3]);
                    if (matcher.matches()) {
                        return matcher.group(1);
                    }
                }
            }
        }
        return null;
    }

    /**
     * Extracts the container id from a cgroup control-group path, mirroring the legacy APM agent's cgroup parsing
     *
     * @see <a href=
     *      "https://github.com/elastic/apm-agent-java/blob/7961a5c7c4fc1fb28de8e41e40bb30f32da384f7/apm-agent-core/src/main/java/co/elastic/apm/agent/impl/metadata/SystemInfo.java">
     *      SystemInfo#findContainerDetails / parseCgroupsLine</a>
     */
    static String parseContainerId(String controlGroupPath) {
        int idSeparator = controlGroupPath.lastIndexOf('/');
        if (idSeparator < 0) {
            return null;
        }
        String idPart = controlGroupPath.substring(idSeparator + 1);
        if (idPart.endsWith(".scope")) {
            idPart = idPart.substring(0, idPart.length() - ".scope".length());
            int dash = idPart.lastIndexOf('-');
            if (dash >= 0) {
                idPart = idPart.substring(dash + 1);
            }
        }
        boolean kubePath = POD_PATH.matcher(controlGroupPath.substring(0, idSeparator)).find();
        if (kubePath
            || CONTAINER_ID_64.matcher(idPart).matches()
            || SHORTENED_UUID.matcher(idPart).matches()
            || AWS_FARGATE.matcher(idPart).matches()) {
            return idPart;
        }
        return null;
    }
}
