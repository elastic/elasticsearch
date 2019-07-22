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
import org.elasticsearch.Build;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;

@SuppressWarnings("unused")
public class SystemdPlugin extends Plugin implements ClusterPlugin {

    private static final Logger logger = LogManager.getLogger(SystemdPlugin.class);

    private final boolean enabled;

    public SystemdPlugin() {
        // our build is configured to only include this module in the package distributions
        assert Build.CURRENT.type() == Build.Type.DEB || Build.CURRENT.type() == Build.Type.RPM : Build.CURRENT.type();
        if (Constants.LINUX == false) {
            enabled = false;
            return;
        }
        final String esSDNotify = System.getenv("ES_SD_NOTIFY");
        if (esSDNotify == null) {
            enabled = false;
            return;
        }
        if ("true".equals(esSDNotify) == false && "false".equals(esSDNotify) == false) {
            throw new RuntimeException("ES_SD_NOTIFY set to unexpected value [" + esSDNotify + "]");
        }
        enabled = "true".equals(esSDNotify);
    }

    @Override
    public void onNodeStarted() {
        if (enabled) {
            // treat failure to notify systemd of readiness as a startup failure
            final int rc = Libsystemd.sd_notify(0, "READY=1");
            logger.trace("sd_notify returned [{}]", rc);
            if (rc < 0) {
                throw new RuntimeException("sd_notify returned error [" + rc + "]");
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (enabled) {
            // do not treat failure to notify systemd of stopping as a failure
            final int rc = Libsystemd.sd_notify(0, "STOPPING=1");
            logger.trace("sd_notify returned [{}]", rc);
            if (rc < 0) {
                logger.warn("sd_notify returned error [{}]", rc);
            }
        }
    }

}
