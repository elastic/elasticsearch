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

import com.carrotsearch.randomizedtesting.RandomizedTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

public class MiniHDFS {

    private static volatile MiniDFSCluster dfs;

    private static String PORT_FILE_NAME = "minihdfs.port";
    private static String PID_FILE_NAME = "minihdfs.pid";

    public static void main(String[] args) throws Exception {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                MiniHDFS.stop();
            }
        });
        start();
    }

    public static int start() throws IOException {
        if (dfs != null) {
            return -1;
        }

        Path basePath = getBasePath();
        Path portPath = basePath.resolve(PORT_FILE_NAME);
        Path pidPath = basePath.resolve(PID_FILE_NAME);

        if (Files.exists(basePath)) {
            RandomizedTest.rmDir(basePath);
        }
        
        Configuration cfg = new Configuration();
        cfg.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, getBasePath().toAbsolutePath().toString());
        // lower default permission
        cfg.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_PERMISSION_KEY, "766");
        dfs = new MiniDFSCluster.Builder(cfg).build();
        int port = dfs.getNameNodePort();

        // write port
        Files.write(portPath, Integer.toString(port).getBytes(StandardCharsets.UTF_8));
        // write pid
        Files.write(pidPath, Long.toString(JvmInfo.jvmInfo().getPid()).getBytes(StandardCharsets.UTF_8));

        System.out.printf(Locale.ROOT, "Started HDFS at %s\n", dfs.getURI());
        System.out.printf(Locale.ROOT, "Port information available at %s\n", portPath.toRealPath());
        System.out.printf(Locale.ROOT, "PID information available at %s\n", pidPath.toRealPath());
        return port;
    }

    private static Path getBasePath() {
        Path tmpFolder = PathUtils.get(System.getProperty("java.io.tmpdir"));
        // "test.build.data"
        String baseFolder = System.getProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA, "es-test/build/test/data");
        return tmpFolder.resolve(baseFolder);
    }

    public static int getPort() throws IOException {
        Path portPath = getBasePath().resolve(PORT_FILE_NAME);
        if (Files.exists(portPath)) {
            return Integer.parseInt(new String(Files.readAllBytes(portPath), StandardCharsets.UTF_8));
        }
        throw new IllegalStateException(String.format(Locale.ROOT, "Cannot find Mini DFS port file at %s ; was '%s' started?", portPath.toAbsolutePath(), MiniHDFS.class));
    }

    public static long getPid() throws Exception {
        Path pidPath = getBasePath().resolve(PID_FILE_NAME);
        if (Files.exists(pidPath)) {
            return Long.parseLong(new String(Files.readAllBytes(pidPath), StandardCharsets.UTF_8));
        }
        throw new IllegalStateException(String.format(Locale.ROOT, "Cannot find Mini DFS pid file at %s ; was '%s' started?", pidPath.toAbsolutePath(), MiniHDFS.class));
    }


    public static void stop() {
        if (dfs != null) {
            dfs.shutdown(true);
            dfs = null;
        }
    }
}
