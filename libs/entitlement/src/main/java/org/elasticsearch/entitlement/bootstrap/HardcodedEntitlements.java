/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.bootstrap;

import org.elasticsearch.core.Booleans;
import org.elasticsearch.entitlement.runtime.policy.Policy;
import org.elasticsearch.entitlement.runtime.policy.PolicyUtils;
import org.elasticsearch.entitlement.runtime.policy.Scope;
import org.elasticsearch.entitlement.runtime.policy.entitlements.CreateClassLoaderEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.Entitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.ExitVMEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.InboundNetworkEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.LoadNativeLibrariesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.ManageThreadsEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.OutboundNetworkEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.ReadJdkImageEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.ReadStoreAttributesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.SetHttpsConnectionPropertiesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.WriteSystemPropertiesEntitlement;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.entitlement.runtime.policy.PathLookup.BaseDir.CONFIG;
import static org.elasticsearch.entitlement.runtime.policy.PathLookup.BaseDir.DATA;
import static org.elasticsearch.entitlement.runtime.policy.PathLookup.BaseDir.LIB;
import static org.elasticsearch.entitlement.runtime.policy.PathLookup.BaseDir.LOGS;
import static org.elasticsearch.entitlement.runtime.policy.PathLookup.BaseDir.MODULES;
import static org.elasticsearch.entitlement.runtime.policy.PathLookup.BaseDir.PLUGINS;
import static org.elasticsearch.entitlement.runtime.policy.PathLookup.BaseDir.SHARED_DATA;
import static org.elasticsearch.entitlement.runtime.policy.PathLookup.BaseDir.SHARED_REPO;
import static org.elasticsearch.entitlement.runtime.policy.Platform.LINUX;
import static org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement.Mode.READ;
import static org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement.Mode.READ_WRITE;

class HardcodedEntitlements {

    private static List<Scope> createServerEntitlements(Path pidFile) {

        List<Scope> serverScopes = new ArrayList<>();
        List<FilesEntitlement.FileData> serverModuleFileDatas = new ArrayList<>();
        Collections.addAll(
            serverModuleFileDatas,
            // Base ES directories
            FilesEntitlement.FileData.ofBaseDirPath(PLUGINS, READ),
            FilesEntitlement.FileData.ofBaseDirPath(MODULES, READ),
            FilesEntitlement.FileData.ofBaseDirPath(CONFIG, READ),
            FilesEntitlement.FileData.ofBaseDirPath(LOGS, READ_WRITE),
            FilesEntitlement.FileData.ofBaseDirPath(LIB, READ),
            FilesEntitlement.FileData.ofBaseDirPath(DATA, READ_WRITE),
            FilesEntitlement.FileData.ofBaseDirPath(SHARED_DATA, READ_WRITE),
            FilesEntitlement.FileData.ofBaseDirPath(SHARED_REPO, READ_WRITE),
            // exclusive settings file
            FilesEntitlement.FileData.ofRelativePath(Path.of("operator/settings.json"), CONFIG, READ_WRITE).withExclusive(true),
            // OS release on Linux
            FilesEntitlement.FileData.ofPath(Path.of("/etc/os-release"), READ).withPlatform(LINUX),
            FilesEntitlement.FileData.ofPath(Path.of("/etc/system-release"), READ).withPlatform(LINUX),
            FilesEntitlement.FileData.ofPath(Path.of("/usr/lib/os-release"), READ).withPlatform(LINUX),
            // read max virtual memory areas
            FilesEntitlement.FileData.ofPath(Path.of("/proc/sys/vm/max_map_count"), READ).withPlatform(LINUX),
            FilesEntitlement.FileData.ofPath(Path.of("/proc/meminfo"), READ).withPlatform(LINUX),
            // load averages on Linux
            FilesEntitlement.FileData.ofPath(Path.of("/proc/loadavg"), READ).withPlatform(LINUX),
            // control group stats on Linux. cgroup v2 stats are in an unpredicable
            // location under `/sys/fs/cgroup`, so unfortunately we have to allow
            // read access to the entire directory hierarchy.
            FilesEntitlement.FileData.ofPath(Path.of("/proc/self/cgroup"), READ).withPlatform(LINUX),
            FilesEntitlement.FileData.ofPath(Path.of("/sys/fs/cgroup/"), READ).withPlatform(LINUX),
            // // io stats on Linux
            FilesEntitlement.FileData.ofPath(Path.of("/proc/self/mountinfo"), READ).withPlatform(LINUX),
            FilesEntitlement.FileData.ofPath(Path.of("/proc/diskstats"), READ).withPlatform(LINUX)
        );
        if (pidFile != null) {
            serverModuleFileDatas.add(FilesEntitlement.FileData.ofPath(pidFile, READ_WRITE));
        }

        Collections.addAll(
            serverScopes,
            new Scope(
                "org.elasticsearch.base",
                List.of(
                    new CreateClassLoaderEntitlement(),
                    new FilesEntitlement(
                        List.of(
                            // necessary due to lack of delegation ES-12382
                            FilesEntitlement.FileData.ofBaseDirPath(SHARED_REPO, READ_WRITE),
                            FilesEntitlement.FileData.ofBaseDirPath(SHARED_DATA, READ_WRITE),
                            FilesEntitlement.FileData.ofBaseDirPath(DATA, READ_WRITE)
                        )
                    )
                )
            ),
            new Scope("org.elasticsearch.xcontent", List.of(new CreateClassLoaderEntitlement())),
            new Scope(
                "org.elasticsearch.server",
                List.of(
                    new ExitVMEntitlement(),
                    new ReadStoreAttributesEntitlement(),
                    new CreateClassLoaderEntitlement(),
                    new InboundNetworkEntitlement(),
                    new LoadNativeLibrariesEntitlement(),
                    new ManageThreadsEntitlement(),
                    new FilesEntitlement(serverModuleFileDatas)
                )
            ),
            new Scope(
                "java.desktop",
                List.of(
                    new LoadNativeLibrariesEntitlement(),
                    new ManageThreadsEntitlement() // For sun.java2d.Disposer. TODO: https://elasticco.atlassian.net/browse/ES-12888
                )
            ),
            new Scope(
                "java.xml",
                List.of(
                    new ReadJdkImageEntitlement(),
                    // java.xml does some reflective stuff that reads calling jars, so allow reading the codebases
                    // of any code in the system so that they can all use java.xml
                    new FilesEntitlement(
                        List.of(
                            FilesEntitlement.FileData.ofBaseDirPath(LIB, READ),
                            FilesEntitlement.FileData.ofBaseDirPath(MODULES, READ),
                            FilesEntitlement.FileData.ofBaseDirPath(PLUGINS, READ)
                        )
                    )
                )
            ),
            new Scope("org.apache.httpcomponents.httpclient", List.of(new OutboundNetworkEntitlement())),
            new Scope(
                "org.apache.lucene.core",
                List.of(
                    new LoadNativeLibrariesEntitlement(),
                    new ManageThreadsEntitlement(),
                    new FilesEntitlement(
                        List.of(
                            FilesEntitlement.FileData.ofBaseDirPath(CONFIG, READ),
                            FilesEntitlement.FileData.ofBaseDirPath(SHARED_DATA, READ_WRITE),
                            FilesEntitlement.FileData.ofBaseDirPath(DATA, READ_WRITE)
                        )
                    )
                )
            ),
            new Scope(
                "org.apache.lucene.misc",
                List.of(
                    new FilesEntitlement(
                        List.of(
                            FilesEntitlement.FileData.ofBaseDirPath(SHARED_DATA, READ_WRITE),
                            FilesEntitlement.FileData.ofBaseDirPath(DATA, READ_WRITE)
                        )
                    ),
                    new ReadStoreAttributesEntitlement()
                )
            ),
            new Scope(
                "org.apache.logging.log4j.core",
                List.of(
                    new ManageThreadsEntitlement(),
                    new FilesEntitlement(List.of(FilesEntitlement.FileData.ofBaseDirPath(LOGS, READ_WRITE)))
                )
            ),
            new Scope(
                "org.elasticsearch.nativeaccess",
                List.of(
                    new LoadNativeLibrariesEntitlement(),
                    new FilesEntitlement(
                        List.of(
                            FilesEntitlement.FileData.ofBaseDirPath(SHARED_DATA, READ_WRITE),
                            FilesEntitlement.FileData.ofBaseDirPath(DATA, READ_WRITE)
                        )
                    )
                )
            )
        );

        // conditionally add FIPS entitlements if FIPS only functionality is enforced
        if (Booleans.parseBoolean(System.getProperty("org.bouncycastle.fips.approved_only"), false)) {
            // From the JSSE reference guide:
            // - If the javax.net.ssl.trustStore property is defined, then the TrustManagerFactory attempts to find a file using
            //   the file name specified by that system property, and uses that file for the KeyStore parameter
            // - If the javax.net.ssl.trustStore property is defined but the specified file does not exist, then a default TrustManager
            //   using an empty keystore is created.
            // - If the javax.net.ssl.trustStore system property was not specified, then:
            //   - if the file java-home/lib/security/jssecacerts exists, that file is used;
            //   - if the file java-home/lib/security/cacerts exists, that file is used;
            //   - if neither of these files exists, then the TLS cipher suite is anonymous, does not perform any authentication,
            //     and thus does not need a truststore.
            String trustStore = System.getProperty("javax.net.ssl.trustStore");
            final List<FilesEntitlement.FileData> trustStoreFiles;
            if (trustStore != null) {
                trustStoreFiles = List.of(FilesEntitlement.FileData.ofPath(Path.of(trustStore), READ));
            } else {
                Path javaHome = Path.of(System.getProperty("java.home"));
                trustStoreFiles = List.of(
                    FilesEntitlement.FileData.ofPath(javaHome.resolve("lib/security/jssecacerts"), READ),
                    FilesEntitlement.FileData.ofPath(javaHome.resolve("lib/security/cacerts"), READ)
                );
            }

            Collections.addAll(
                serverScopes,
                new Scope(
                    "org.bouncycastle.fips.tls",
                    List.of(
                        new FilesEntitlement(trustStoreFiles),
                        new ManageThreadsEntitlement(),
                        new OutboundNetworkEntitlement()
                    )
                ),
                new Scope(
                    "org.bouncycastle.fips.core",
                    // read to lib dir is required for checksum validation
                    List.of(
                        new FilesEntitlement(List.of(FilesEntitlement.FileData.ofBaseDirPath(LIB, READ))),
                        new ManageThreadsEntitlement()
                    )
                )
            );
        }
        return serverScopes;
    }

    static Policy serverPolicy(Path pidFile, Policy serverPolicyPatch) {
        var serverScopes = createServerEntitlements(pidFile);
        return new Policy(
            "server",
            serverPolicyPatch == null ? serverScopes : PolicyUtils.mergeScopes(serverScopes, serverPolicyPatch.scopes())
        );
    }

    // agents run without a module, so this is a special hack for the apm agent
    // this should be removed once https://github.com/elastic/elasticsearch/issues/109335 is completed
    // See also modules/apm/src/main/plugin-metadata/entitlement-policy.yaml
    static List<Entitlement> agentEntitlements() {
        return List.of(
            new CreateClassLoaderEntitlement(),
            new ManageThreadsEntitlement(),
            new SetHttpsConnectionPropertiesEntitlement(),
            new OutboundNetworkEntitlement(),
            new WriteSystemPropertiesEntitlement(Set.of("AsyncProfiler.safemode")),
            new LoadNativeLibrariesEntitlement(),
            new FilesEntitlement(
                List.of(
                    FilesEntitlement.FileData.ofBaseDirPath(LOGS, READ_WRITE),
                    FilesEntitlement.FileData.ofPath(Path.of("/proc/meminfo"), READ),
                    FilesEntitlement.FileData.ofPath(Path.of("/sys/fs/cgroup/"), READ)
                )
            )
        );
    }
}
