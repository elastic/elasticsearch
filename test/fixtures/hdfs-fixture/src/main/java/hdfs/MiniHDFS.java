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

package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Locale;

import java.lang.management.ManagementFactory;

/**
 * MiniHDFS test fixture. There is a CLI tool, but here we can
 * easily properly setup logging, avoid parsing JSON, etc.
 */
public class MiniHDFS {

    private static String PORT_FILE_NAME = "ports";
    private static String PID_FILE_NAME = "pid";

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
           throw new IllegalArgumentException("MiniHDFS <baseDirectory>");
        }
        // configure Paths
        Path baseDir = Paths.get(args[0]);
        // hadoop-home/, so logs will not complain
        if (System.getenv("HADOOP_HOME") == null) {
            Path hadoopHome = baseDir.resolve("hadoop-home");
            Files.createDirectories(hadoopHome);
            System.setProperty("hadoop.home.dir", hadoopHome.toAbsolutePath().toString());
        }
        // hdfs-data/, where any data is going
        Path hdfsHome = baseDir.resolve("hdfs-data");

        // start cluster
        Configuration cfg = new Configuration();
        cfg.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, hdfsHome.toAbsolutePath().toString());
        // lower default permission: TODO: needed?
        cfg.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_PERMISSION_KEY, "766");
        // TODO: remove hardcoded port!
        MiniDFSCluster dfs = new MiniDFSCluster.Builder(cfg).nameNodePort(9999).build();

        // write our PID file
        Path tmp = Files.createTempFile(baseDir, null, null);
        String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
        Files.write(tmp, pid.getBytes(StandardCharsets.UTF_8));
        Files.move(tmp, baseDir.resolve(PID_FILE_NAME), StandardCopyOption.ATOMIC_MOVE);

        // write our port file
        tmp = Files.createTempFile(baseDir, null, null);
        Files.write(tmp, Integer.toString(dfs.getNameNodePort()).getBytes(StandardCharsets.UTF_8));
        Files.move(tmp, baseDir.resolve(PORT_FILE_NAME), StandardCopyOption.ATOMIC_MOVE);
    }
}
