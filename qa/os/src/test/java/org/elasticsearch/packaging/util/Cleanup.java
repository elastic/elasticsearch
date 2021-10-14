/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.elasticsearch.packaging.test.PackagingTestCase.getRootTempDir;
import static org.elasticsearch.packaging.util.FileUtils.lsGlob;
import static org.elasticsearch.packaging.util.Platforms.isDPKG;
import static org.elasticsearch.packaging.util.Platforms.isRPM;

public class Cleanup {

    protected static final Logger logger = LogManager.getLogger(Cleanup.class);

    private static final List<String> ELASTICSEARCH_FILES_LINUX = Arrays.asList(
        "/usr/share/elasticsearch",
        "/etc/elasticsearch/elasticsearch.keystore",
        "/etc/elasticsearch",
        "/var/lib/elasticsearch",
        "/var/log/elasticsearch",
        "/etc/default/elasticsearch",
        "/etc/sysconfig/elasticsearch",
        "/var/run/elasticsearch",
        "/usr/share/doc/elasticsearch",
        "/usr/lib/systemd/system/elasticsearch.conf",
        "/usr/lib/tmpfiles.d/elasticsearch.conf",
        "/usr/lib/sysctl.d/elasticsearch.conf"
    );

    // todo
    private static final List<String> ELASTICSEARCH_FILES_WINDOWS = Collections.emptyList();

    public static void cleanEverything() throws Exception {
        final Shell sh = new Shell();

        // kill elasticsearch processes
        Platforms.onLinux(() -> {
            sh.runIgnoreExitCode("pkill -u elasticsearch");
            sh.runIgnoreExitCode("ps aux | grep -i 'org.elasticsearch.bootstrap.Elasticsearch' | awk {'print $2'} | xargs kill -9");
        });

        Platforms.onWindows(() -> {
            // the view of processes returned by Get-Process doesn't expose command line arguments, so we use WMI here
            sh.runIgnoreExitCode(
                "Get-WmiObject Win32_Process | "
                    + "Where-Object { $_.CommandLine -Match 'org.elasticsearch.bootstrap.Elasticsearch' } | "
                    + "ForEach-Object { $_.Terminate() }"
            );
        });

        Platforms.onLinux(Cleanup::purgePackagesLinux);

        // remove elasticsearch users
        Platforms.onLinux(() -> {
            sh.runIgnoreExitCode("userdel elasticsearch");
            sh.runIgnoreExitCode("groupdel elasticsearch");
        });
        // when we run es as a role user on windows, add the equivalent here
        // delete files that may still exist

        lsGlob(getRootTempDir(), "elasticsearch*").forEach(Platforms.WINDOWS ? FileUtils::rmWithRetries : FileUtils::rm);
        final List<String> filesToDelete = Platforms.WINDOWS ? ELASTICSEARCH_FILES_WINDOWS : ELASTICSEARCH_FILES_LINUX;
        // windows needs leniency due to asinine releasing of file locking async from a process exiting
        Consumer<? super Path> rm = Platforms.WINDOWS ? FileUtils::rmWithRetries : FileUtils::rm;
        filesToDelete.stream().map(Paths::get).filter(Files::exists).forEach(rm);
    }

    private static void purgePackagesLinux() {
        final Shell sh = new Shell();

        if (isRPM()) {
            // Doing rpm erase on both packages in one command will remove neither since both cannot be installed
            // this may leave behind config files in /etc/elasticsearch, but a later step in this cleanup will get them
            sh.runIgnoreExitCode("rpm --quiet -e elasticsearch");
            sh.runIgnoreExitCode("rpm --quiet -e elasticsearch-oss");
        }

        if (isDPKG()) {
            sh.runIgnoreExitCode("dpkg --purge elasticsearch elasticsearch-oss");
        }
    }
}
