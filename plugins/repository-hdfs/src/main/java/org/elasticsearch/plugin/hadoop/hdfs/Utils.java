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
package org.elasticsearch.plugin.hadoop.hdfs;

import java.net.URL;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.DomainCombiner;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;

import org.elasticsearch.SpecialPermission;

public abstract class Utils {

    protected static AccessControlContext hadoopACC() {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            // unprivileged code such as scripts do not have SpecialPermission
            sm.checkPermission(new SpecialPermission());
        }

        return AccessController.doPrivileged(new PrivilegedAction<AccessControlContext>() {
            @Override
            public AccessControlContext run() {
                return new AccessControlContext(AccessController.getContext(), new HadoopDomainCombiner());
            }
        });
    }

    private static class HadoopDomainCombiner implements DomainCombiner {

        private static String BASE_LIB = detectLibFolder();

        @Override
        public ProtectionDomain[] combine(ProtectionDomain[] currentDomains, ProtectionDomain[] assignedDomains) {
            for (ProtectionDomain pd : assignedDomains) {
                if (pd.getCodeSource().getLocation().toString().startsWith(BASE_LIB)) {
                    return assignedDomains;
                }
            }

            return currentDomains;
        }
    }

    static String detectLibFolder() {
        ClassLoader cl = Utils.class.getClassLoader();

        // we could get the URL from the URLClassloader directly
        // but that can create issues when running the tests from the IDE
        // we could detect that by loading resources but that as well relies on
        // the JAR URL
        String classToLookFor = HdfsPlugin.class.getName().replace(".", "/").concat(".class");
        URL classURL = cl.getResource(classToLookFor);
        if (classURL == null) {
            throw new IllegalStateException("Cannot detect itself; something is wrong with this ClassLoader " + cl);
        }

        String base = classURL.toString();

        // extract root
        // typically a JAR URL
        int index = base.indexOf("!/");
        if (index > 0) {
            base = base.substring(0, index);
            // remove its prefix (jar:)
            base = base.substring(4);
            // remove the trailing jar
            index = base.lastIndexOf("/");
            base = base.substring(0, index + 1);
        }
        // not a jar - something else, do a best effort here
        else {
            // remove the class searched
            base = base.substring(0, base.length() - classToLookFor.length());
        }

        // append /
        if (!base.endsWith("/")) {
            base = base.concat("/");
        }

        return base;
    }
}