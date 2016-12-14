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

import java.security.Permission;

/**
 * This class starts elasticsearch.
 */
public final class Elasticsearch {

    /** no instantiation */
    private Elasticsearch() {}

    /**
     * Main entry point for starting elasticsearch
     */
    public static void main(String[] args) throws StartupError {
        // we want the JVM to think there is a security manager installed so that if internal policy decisions that would be based on the
        // presence of a security manager or lack thereof act as if there is a security manager present (e.g., DNS cache policy)
        System.setSecurityManager(new SecurityManager() {
            @Override
            public void checkPermission(Permission perm) {
                // grant all permissions so that we can later set the security manager to the one that we want
            }
        });
        try {
            Bootstrap.init(args);
        } catch (Throwable t) {
            // format exceptions to the console in a special way
            // to avoid 2MB stacktraces from guice, etc.
            throw new StartupError(t);
        }
    }

    /**
     * Required method that's called by Apache Commons procrun when
     * running as a service on Windows, when the service is stopped.
     *
     * http://commons.apache.org/proper/commons-daemon/procrun.html
     *
     * NOTE: If this method is renamed and/or moved, make sure to update service.bat!
     */
    static void close(String[] args) {
        Bootstrap.stop();
    }

}
