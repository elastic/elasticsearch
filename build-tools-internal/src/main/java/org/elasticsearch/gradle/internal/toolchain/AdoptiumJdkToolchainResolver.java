/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.toolchain;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.gradle.jvm.toolchain.JavaLanguageVersion;
import org.gradle.jvm.toolchain.JavaToolchainDownload;
import org.gradle.jvm.toolchain.JavaToolchainRequest;
import org.gradle.jvm.toolchain.JvmVendorSpec;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.StreamSupport;

import static org.gradle.jvm.toolchain.JavaToolchainDownload.fromUri;

public abstract class AdoptiumJdkToolchainResolver extends AbstractCustomJavaToolchainResolver {

    // package protected for better testing
    final Map<AdoptiumVersionRequest, Optional<String>> CACHED_RELEASES = new ConcurrentHashMap<>();

    @Override
    public Optional<JavaToolchainDownload> resolve(JavaToolchainRequest request) {
        if (requestIsSupported(request) == false) {
            return Optional.empty();
        }
        AdoptiumVersionRequest versionRequestKey = toVersionRequest(request);
        Optional<String> versionInfo = CACHED_RELEASES.computeIfAbsent(
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

    private Optional<String> resolveAvailableVersion(AdoptiumVersionRequest requestKey) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            int languageVersion = requestKey.languageVersion.asInt();
            URL source = new URL(
                "https://api.adoptium.net/v3/info/release_names?architecture="
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
            JsonNode versionsNode = jsonNode.get("releases");
            return StreamSupport.stream(versionsNode.spliterator(), false).map(JsonNode::textValue).findFirst();
        } catch (FileNotFoundException e) {
            // request combo not supported (e.g. aarch64 + windows
            return Optional.empty();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private URI resolveDownloadURI(AdoptiumVersionRequest request, String version) {
        return URI.create(
            "https://api.adoptium.net/v3/binary/version/"
                + version
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

    record AdoptiumVersionRequest(String platform, String arch, JavaLanguageVersion languageVersion) {}
}
