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

import org.elasticsearch.Build;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class OtelSdkResourceTests extends ESTestCase {

    private static final UnaryOperator<String> NO_ENV = key -> null;

    public void testOtelSdkResourceBuilding() {
        Settings settings = Settings.builder()
            .put("node.name", "node-7")
            .put("telemetry.resource.project.id", "abc-123")
            .put("telemetry.resource.orchestrator.cluster.name", "es-prod-eu-west-1")
            .build();

        Resource resource = OtelSdkResource.get(settings, NO_ENV);

        assertThat(resource.getAttribute(AttributeKey.stringKey("service.name")), is("self-managed-elasticsearch"));
        assertThat(resource.getAttribute(AttributeKey.stringKey("service.version")), is(Build.current().version()));
        assertThat(resource.getAttribute(AttributeKey.stringKey("process.runtime.name")), is("Java"));
        assertThat(resource.getAttribute(AttributeKey.stringKey("process.runtime.version")), is(System.getProperty("java.version")));
        assertThat(resource.getAttribute(AttributeKey.stringKey("telemetry.distro.name")), is("elasticsearch-otel-sdk"));
        assertThat(resource.getAttribute(AttributeKey.stringKey("telemetry.distro.version")), is(Build.current().version()));
        assertThat(resource.getAttribute(AttributeKey.stringKey("service.instance.id")), is("node-7"));
        assertThat(resource.getAttribute(AttributeKey.stringKey("project.id")), is("abc-123"));
        assertThat(resource.getAttribute(AttributeKey.stringKey("orchestrator.cluster.name")), is("es-prod-eu-west-1"));
    }

    public void testOtelSdkResourceOverride() {
        Settings settings = Settings.builder().put("telemetry.resource.service.name", "operator-supplied-name").build();

        Resource resource = OtelSdkResource.get(settings, NO_ENV);

        assertThat(resource.getAttribute(AttributeKey.stringKey("service.name")), is("operator-supplied-name"));
    }

    public void testHostOsProcessAttributes() {
        Resource resource = OtelSdkResource.get(Settings.EMPTY, NO_ENV);

        assertThat(resource.getAttribute(AttributeKey.stringKey("host.arch")), is(System.getProperty("os.arch")));
        assertThat(resource.getAttribute(AttributeKey.stringKey("os.type")), is(System.getProperty("os.name")));
        assertThat(resource.getAttribute(AttributeKey.longKey("process.pid")), is(ProcessHandle.current().pid()));
        ProcessHandle.current()
            .info()
            .command()
            .ifPresent(command -> assertThat(resource.getAttribute(AttributeKey.stringKey("process.executable.path")), is(command)));
    }

    public void testKubernetesAttributesFromEnv() {
        UnaryOperator<String> env = Map.of(
            "KUBERNETES_NAMESPACE",
            "es",
            "KUBERNETES_NODE_NAME",
            "node-a",
            "KUBERNETES_POD_NAME",
            "es-0",
            "KUBERNETES_POD_UID",
            "uid-123",
            "HOSTNAME",
            "es-0-host"
        )::get;

        Resource resource = OtelSdkResource.get(Settings.EMPTY, env);

        assertThat(resource.getAttribute(AttributeKey.stringKey("k8s.namespace.name")), is("es"));
        assertThat(resource.getAttribute(AttributeKey.stringKey("k8s.node.name")), is("node-a"));
        assertThat(resource.getAttribute(AttributeKey.stringKey("k8s.pod.name")), is("es-0"));
        assertThat(resource.getAttribute(AttributeKey.stringKey("k8s.pod.uid")), is("uid-123"));
        assertThat(resource.getAttribute(AttributeKey.stringKey("host.name")), is("es-0-host"));
    }

    public void testKubernetesAttributesOmittedWhenAbsent() {
        Resource resource = OtelSdkResource.get(Settings.EMPTY, NO_ENV);

        assertThat(resource.getAttribute(AttributeKey.stringKey("k8s.namespace.name")), is(nullValue()));
        assertThat(resource.getAttribute(AttributeKey.stringKey("k8s.node.name")), is(nullValue()));
        assertThat(resource.getAttribute(AttributeKey.stringKey("k8s.pod.name")), is(nullValue()));
        assertThat(resource.getAttribute(AttributeKey.stringKey("k8s.pod.uid")), is(nullValue()));
        assertThat(resource.getAttribute(AttributeKey.stringKey("host.name")), is(nullValue()));
    }

    public void testDeploymentEnvironment() {
        assertThat(OtelSdkResource.get(Settings.EMPTY, NO_ENV).getAttribute(AttributeKey.stringKey("deployment.environment")), is("dev"));

        Settings settings = Settings.builder().put("telemetry.agent.environment", "qa").build();
        assertThat(OtelSdkResource.get(settings, NO_ENV).getAttribute(AttributeKey.stringKey("deployment.environment")), is("qa"));
    }

    public void testParseContainerId() {
        String dockerId = "3741401135a8d27237e2fb9c0fb2ecd93922c0d1dd708345451e479613f8d4ae";
        assertThat(OtelSdkResource.parseContainerId("/docker/" + dockerId), is(dockerId));

        String systemdId = "b15a5bdedd2e7645c3be271364324321b908314e4c77857bbfd32a041148c07f";
        assertThat(OtelSdkResource.parseContainerId("/system.slice/docker-" + systemdId + ".scope"), is(systemdId));

        String k8sId = "244a65edefdffe31685c42317c9054e71dc1193048cf9459e2a4dd35cbc1dba4";
        assertThat(OtelSdkResource.parseContainerId("/kubepods/besteffort/pod0e886e9a-3879-45f9-b44d-86ef9df03224/" + k8sId), is(k8sId));

        String cgroupV2Id = "0f99ad5f45163ed14ab8eaf92ed34bb4a631d007f8755a7d79be614bcb0df0ef";
        assertThat(
            OtelSdkResource.parseContainerId(
                "/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod90d81341_92de_11e7_8cf2_507b9d4141fa.slice/cri-containerd-"
                    + cgroupV2Id
                    + ".scope"
            ),
            is(cgroupV2Id)
        );

        assertThat(OtelSdkResource.parseContainerId("/"), is(nullValue()));
    }

    public void testParseMountInfoContainerId() {
        String dockerId = "6548c6863fb748e72d1e2a4f824fde92f720952d062dede1318c2d6219a672d6";
        assertThat(
            OtelSdkResource.parseMountInfoContainerId(
                List.of(
                    "1608 1584 0:52 / / rw,relatime - overlay overlay rw",
                    "1620 1608 254:1 /var/lib/docker/containers/" + dockerId + "/hostname /etc/hostname rw,relatime - ext4 /dev/sda1 rw"
                )
            ),
            is(dockerId)
        );

        String containerdId = "26a006f558da58874bc37863efe9d2b5d715afc54453d95b22a7809a4e65566c";
        assertThat(
            OtelSdkResource.parseMountInfoContainerId(
                List.of(
                    "10740 10112 8:3 /var/lib/containerd/io.containerd.grpc.v1.cri/sandboxes/"
                        + containerdId
                        + "/hostname /etc/hostname ro,relatime - ext4 /dev/sda3 rw"
                )
            ),
            is(containerdId)
        );

        assertThat(
            OtelSdkResource.parseMountInfoContainerId(List.of("1608 1584 0:52 / / rw,relatime - overlay overlay rw")),
            is(nullValue())
        );
    }
}
