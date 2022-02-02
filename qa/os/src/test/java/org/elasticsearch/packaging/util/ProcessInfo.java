/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.util;

import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

/**
 * Encapsulates the fetching of information about a running process.
 * <p>
 * This is helpful on stripped-down Docker images where, in order to fetch full information about a process,
 * we have to consult <code>/proc</code>. Although this class hides the implementation details, it only
 * works in Linux containers. At the moment that isn't a problem, because we only publish Docker images
 * for Linux.
 */
public record ProcessInfo(int pid, int uid, int gid, String username, String group) {

    /**
     * Fetches process information about <code>command</code>, using <code>sh</code> to execute commands.
     *
     * @return a populated <code>ProcessInfo</code> object
     */
    public static ProcessInfo getProcessInfo(Shell sh, String command) {
        final List<String> processes = sh.run("pgrep " + command).stdout().lines().collect(Collectors.toList());

        assertThat("Expected a single process", processes, hasSize(1));

        // Ensure we actually have a number
        final int pid = Integer.parseInt(processes.get(0).trim());

        int uid = -1;
        int gid = -1;

        for (String line : sh.run("cat /proc/" + pid + "/status | grep '^[UG]id:'").stdout().split("\\n")) {
            final String[] fields = line.split("\\s+");

            if (fields[0].equals("Uid:")) {
                uid = Integer.parseInt(fields[1]);
            } else {
                gid = Integer.parseInt(fields[1]);
            }
        }

        final String username = sh.run("getent passwd " + uid + " | cut -f1 -d:").stdout().trim();
        final String group = sh.run("getent group " + gid + " | cut -f1 -d:").stdout().trim();

        return new ProcessInfo(pid, uid, gid, username, group);
    }
}
