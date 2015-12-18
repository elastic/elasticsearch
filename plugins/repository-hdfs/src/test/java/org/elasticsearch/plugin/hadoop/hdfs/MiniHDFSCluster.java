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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.elasticsearch.common.SuppressForbidden;

import java.io.File;

public class MiniHDFSCluster {

    @SuppressForbidden(reason = "Hadoop is messy")
    public static void main(String[] args) throws Exception {
        FileUtil.fullyDelete(new File(System.getProperty("test.build.data", "build/test/data"), "dfs/"));
        // MiniHadoopClusterManager.main(new String[] { "-nomr" });
        Configuration cfg = new Configuration();
        cfg.set(DataNode.DATA_DIR_PERMISSION_KEY, "666");
        cfg.set("dfs.replication", "0");
        MiniDFSCluster dfsCluster = new MiniDFSCluster(cfg, 1, true, null);
        FileSystem fs = dfsCluster.getFileSystem();
        System.out.println(fs.getClass());
        System.out.println(fs.getUri());
        System.out.println(dfsCluster.getHftpFileSystem().getClass());

        // dfsCluster.shutdown();
    }
}
