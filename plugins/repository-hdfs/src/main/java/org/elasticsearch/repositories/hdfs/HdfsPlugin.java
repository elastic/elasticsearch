/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories.hdfs;

import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.SecurityUtil;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Collections;
import java.util.Map;

public final class HdfsPlugin extends Plugin implements RepositoryPlugin {

    // initialize some problematic classes
    static {
        eagerInitSecurityUtil();
    }

    private static Void eagerInitSecurityUtil() {
        /*
         * Hadoop RPC wire serialization uses ProtocolBuffers. All proto classes for Hadoop
         * come annotated with configurations that denote information about if they support
         * certain security options like Kerberos, and how to send information with the
         * message to support that authentication method. SecurityUtil creates a service loader
         * in a static field during its clinit. This loader provides the implementations that
         * pull the security information for each proto class. The service loader sources its
         * services from the current thread's context class loader, which must contain the Hadoop
         * jars. Since plugins don't execute with their class loaders installed as the thread's
         * context class loader, we need to install the loader briefly, allow the util to be
         * initialized, then restore the old loader since we don't actually own this thread.
         */
        ClassLoader oldCCL = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(HdfsRepository.class.getClassLoader());
            KerberosInfo info = SecurityUtil.getKerberosInfo(ClientNamenodeProtocolPB.class, null);
            // Make sure that the correct class loader was installed.
            if (info == null) {
                throw new RuntimeException(
                    "Could not initialize SecurityUtil: " + "Unable to find services for [org.apache.hadoop.security.SecurityInfo]"
                );
            }
        } finally {
            Thread.currentThread().setContextClassLoader(oldCCL);
        }
        return null;
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(
        Environment env,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        BigArrays bigArrays,
        RecoverySettings recoverySettings,
        RepositoriesMetrics repositoriesMetrics
    ) {
        return Collections.singletonMap(
            "hdfs",
            (projectId, metadata) -> new HdfsRepository(
                projectId,
                metadata,
                env,
                namedXContentRegistry,
                clusterService,
                bigArrays,
                recoverySettings
            )
        );
    }
}
