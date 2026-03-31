/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.fixtures

import org.gradle.api.Plugin

abstract class AbstractGradleInternalPluginFuncTest extends AbstractJavaGradleFuncTest {

    abstract <T extends Plugin> Class<T> getPluginClassUnderTest();

    def setup() {
        settingsFile.text = """
        plugins {
            id 'elasticsearch.java-toolchain'
        }

        toolchainManagement {
          jvm {
            javaRepositories {
              repository('bundledOracleOpendJdk') {
                resolverClass = org.elasticsearch.gradle.internal.toolchain.OracleOpenJdkToolchainResolver
              }
              repository('adoptiumJdks') {
                resolverClass = org.elasticsearch.gradle.internal.toolchain.AdoptiumJdkToolchainResolver
              }
              repository('archivedOracleJdks') {
                resolverClass = org.elasticsearch.gradle.internal.toolchain.ArchivedOracleJdkToolchainResolver
              }
            }
          }
        }
        """ + settingsFile.text

        buildFile << """
        import ${getPluginClassUnderTest().getName()}

        plugins {
          // bring in build-tools-internal onto the classpath
          id 'elasticsearch.global-build-info'
        }
        // internally used plugins do not have a plugin id as they are
        // not intended to be used directly from build scripts
        plugins.apply(${getPluginClassUnderTest().getSimpleName()})

        """
    }
}
