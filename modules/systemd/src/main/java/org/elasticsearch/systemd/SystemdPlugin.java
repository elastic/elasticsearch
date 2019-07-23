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

package org.elasticsearch.systemd;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;
import org.elasticsearch.Assertions;
import org.elasticsearch.Build;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;

public class SystemdPlugin extends Plugin implements ClusterPlugin {

    private static final Logger logger = LogManager.getLogger(SystemdPlugin.class);

    private final boolean enabled;

    final boolean isEnabled() {
        return enabled;
    }

    @SuppressWarnings("unused")
    public SystemdPlugin() {
        this(true, Constants.LINUX, System.getenv("ES_SD_NOTIFY"));
    }

    SystemdPlugin(final boolean assertIsPackageDistribution, final boolean isLinux, final String esSDNotify) {
        if (Assertions.ENABLED && assertIsPackageDistribution) {
            // our build is configured to only include this module in the package distributions
            assert Build.CURRENT.type() == Build.Type.DEB || Build.CURRENT.type() == Build.Type.RPM : Build.CURRENT.type();
        }
        if (isLinux == false || esSDNotify == null) {
            enabled = false;
            return;
        }
        if (Boolean.TRUE.toString().equals(esSDNotify) == false && Boolean.FALSE.toString().equals(esSDNotify) == false) {
            throw new RuntimeException("ES_SD_NOTIFY set to unexpected value [" + esSDNotify + "]");
        }
        enabled = Boolean.TRUE.toString().equals(esSDNotify);
    }

    int sd_notify(@SuppressWarnings("SameParameterValue") final int unset_environment, final String state) {
        return Libsystemd.sd_notify(unset_environment, state);
    }

    @Override
    public void onNodeStarted() {
        if (enabled) {
            final int rc = sd_notify(0, "READY=1");
            logger.trace("sd_notify returned [{}]", rc);
            if (rc < 0) {
                // treat failure to notify systemd of readiness as a startup failure
                throw new RuntimeException("sd_notify returned error [" + rc + "]");
            }
        }
    }

    @Override
    public void close() {
        if (enabled) {
            final int rc = sd_notify(0, "STOPPING=1");
            logger.trace("sd_notify returned [{}]", rc);
            if (rc < 0) {
                // do not treat failure to notify systemd of stopping as a failure
                logger.warn("sd_notify returned error [{}]", rc);
            }
        }
    }

}
