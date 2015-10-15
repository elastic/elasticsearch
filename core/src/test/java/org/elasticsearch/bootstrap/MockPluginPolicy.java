/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.bootstrap;

import com.carrotsearch.randomizedtesting.RandomizedRunner;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.common.logging.Loggers;
import org.junit.Assert;

import java.net.URL;
import java.security.CodeSource;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Policy;
import java.security.ProtectionDomain;
import java.security.cert.Certificate;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Simulates in unit tests per-plugin permissions.
 * Unit tests for plugins do not have a proper plugin structure,
 * so we don't know which codebases to apply the permission to.
 * <p>
 * As an approximation, we just exclude es/test/framework classes,
 * because they will be present in stacks and fail tests for the 
 * simple case where an AccessController block is missing, because
 * java security checks every codebase in the stacktrace, and we
 * are sure to pollute it.
 */
final class MockPluginPolicy extends Policy {
    final ESPolicy standardPolicy;
    final PermissionCollection extraPermissions;
    final Set<CodeSource> excludedSources;

    /**
     * Create a new MockPluginPolicy with dynamic {@code permissions} and
     * adding the extra plugin permissions from {@code insecurePluginProp} to
     * all code except test classes.
     */
    MockPluginPolicy(PermissionCollection standard, PermissionCollection extra) throws Exception {
        // the hack begins!

        this.standardPolicy = new ESPolicy(standard, Collections.<String,PermissionCollection>emptyMap());
        this.extraPermissions = extra;

        excludedSources = new HashSet<CodeSource>();
        // exclude some obvious places
        // es core
        excludedSources.add(Bootstrap.class.getProtectionDomain().getCodeSource());
        // es test framework
        excludedSources.add(getClass().getProtectionDomain().getCodeSource());
        // lucene test framework
        excludedSources.add(LuceneTestCase.class.getProtectionDomain().getCodeSource());
        // test runner
        excludedSources.add(RandomizedRunner.class.getProtectionDomain().getCodeSource());
        // junit library
        excludedSources.add(Assert.class.getProtectionDomain().getCodeSource());
        // scripts
        excludedSources.add(new CodeSource(new URL("file:" + BootstrapInfo.UNTRUSTED_CODEBASE), (Certificate[])null));

        Loggers.getLogger(getClass()).debug("Apply extra permissions [{}] excluding codebases [{}]", extraPermissions, excludedSources);
    }

    @Override
    public boolean implies(ProtectionDomain domain, Permission permission) {
        CodeSource codeSource = domain.getCodeSource();
        // codesource can be null when reducing privileges via doPrivileged()
        if (codeSource == null) {
            return false;
        }

        if (standardPolicy.implies(domain, permission)) {
            return true;
        } else if (excludedSources.contains(codeSource) == false &&
                   codeSource.toString().contains("test-classes") == false) {
            return extraPermissions.implies(permission);
        } else {
            return false;
        }
    }
}
