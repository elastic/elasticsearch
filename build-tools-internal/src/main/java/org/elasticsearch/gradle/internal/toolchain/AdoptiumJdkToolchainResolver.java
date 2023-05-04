/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.toolchain;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.compress.utils.Lists;
import org.gradle.jvm.toolchain.JavaLanguageVersion;
import org.gradle.jvm.toolchain.JavaToolchainDownload;
import org.gradle.jvm.toolchain.JavaToolchainRequest;
import org.gradle.jvm.toolchain.JvmVendorSpec;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.gradle.jvm.toolchain.JavaToolchainDownload.fromUri;

public abstract class AdoptiumJdkToolchainResolver extends AbstractCustomJavaToolchainResolver {

    // package protected for better testing
    final Map<AdoptiumVersionRequest, Optional<AdoptiumVersionInfo>> CACHED_SEMVERS = new ConcurrentHashMap<>();

    @Override
    public Optional<JavaToolchainDownload> resolve(JavaToolchainRequest request) {
        if (requestIsSupported(request) == false) {
            return Optional.empty();
        }
        AdoptiumVersionRequest versionRequestKey = toVersionRequest(request);
        Optional<AdoptiumVersionInfo> versionInfo = CACHED_SEMVERS.computeIfAbsent(
            versionRequestKey,
            (r) -> resolveAvailableVersion(versionRequestKey)
        );

        return versionInfo.map(v -> fromUri(resolveDownloadURI(versionRequestKey, v)));
    }

    private AdoptiumVersionRequest toVersionRequest(JavaToolchainRequest request) {
        String platform = toOsString(request.getBuildPlatform().getOperatingSystem(), JvmVendorSpec.ADOPTIUM);
        String arch = toArchString(request.getBuildPlatform().getArchitecture());
        JavaLanguageVersion javaLanguageVersion = request.getJavaToolchainSpec().getLanguageVersion().get();
        return new AdoptiumVersionRequest(platform, arch, javaLanguageVersion);
    }

    private Optional<AdoptiumVersionInfo> resolveAvailableVersion(AdoptiumVersionRequest requestKey) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            int languageVersion = requestKey.languageVersion.asInt();
            URL source = new URL(
                "https://api.adoptium.net/v3/info/release_versions?architecture="
                    + requestKey.arch
                    + "&image_type=jdk&os="
                    + requestKey.platform
                    + "&project=jdk&release_type=ga"
                    + "&version=["
                    + languageVersion
                    + ","
                    + (languageVersion + 1)
                    + ")"
            );
            JsonNode jsonNode = mapper.readTree(source);
            JsonNode versionsNode = jsonNode.get("versions");
            return Optional.of(
                Lists.newArrayList(versionsNode.iterator())
                    .stream()
                    .map(node -> toVersionInfo(node))
                    .sorted(Comparator.comparing(AdoptiumVersionInfo::semver).reversed())
                    .findFirst()
                    .get()
            );
        } catch (FileNotFoundException e) {
            // request combo not supported (e.g. aarch64 + windows
            return Optional.empty();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private AdoptiumVersionInfo toVersionInfo(JsonNode node) {
        return new AdoptiumVersionInfo(
            node.get("build").asInt(),
            node.get("major").asInt(),
            node.get("minor").asInt(),
            node.get("openjdk_version").asText(),
            node.get("security").asInt(),
            node.get("semver").asText()
        );
    }

    private URI resolveDownloadURI(AdoptiumVersionRequest request, AdoptiumVersionInfo versionInfo) {
        return URI.create(
            "https://api.adoptium.net/v3/binary/version/jdk-"
                + versionInfo.openjdkVersion
                + "/"
                + request.platform
                + "/"
                + request.arch
                + "/jdk/hotspot/normal/eclipse?project=jdk"
        );
    }

    /**
     * Check if request can be full-filled by this resolver:
     * 1. vendor must be "any" or adoptium
     */
    private boolean requestIsSupported(JavaToolchainRequest request) {
        return anyVendorOr(request.getJavaToolchainSpec().getVendor().get(), JvmVendorSpec.ADOPTIUM);
    }

    record AdoptiumVersionInfo(int build, int major, int minor, String openjdkVersion, int security, String semver) {}

    record AdoptiumVersionRequest(String platform, String arch, JavaLanguageVersion languageVersion) {}
}
