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
public class ProcessInfo {
    public final int pid;
    public final int uid;
    public final int gid;
    public final String username;
    public final String group;

    public ProcessInfo(int pid, int uid, int gid, String username, String group) {
        this.pid = pid;
        this.uid = uid;
        this.gid = gid;
        this.username = username;
        this.group = group;
    }

    /**
     * Fetches process information about <code>command</code>, using <code>sh</code> to execute commands.
     * @return a populated <code>ProcessInfo</code> object
     */
    public static ProcessInfo getProcessInfo(Shell sh, String command) {
        final List<String> processes = sh.run("pgrep " + command).stdout.lines().collect(Collectors.toList());

        assertThat("Expected a single process", processes, hasSize(1));

        // Ensure we actually have a number
        final int pid = Integer.parseInt(processes.get(0).trim());

        int uid = -1;
        int gid = -1;

        for (String line : sh.run("cat /proc/" + pid + "/status | grep '^[UG]id:'").stdout.split("\\n")) {
            final String[] fields = line.split("\\s+");

            if (fields[0].equals("Uid:")) {
                uid = Integer.parseInt(fields[1]);
            } else {
                gid = Integer.parseInt(fields[1]);
            }
        }

        final String username = sh.run("getent passwd " + uid + " | cut -f1 -d:").stdout.trim();
        final String group = sh.run("getent group " + gid + " | cut -f1 -d:").stdout.trim();

        return new ProcessInfo(pid, uid, gid, username, group);
    }
}
