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
package org.elasticsearch.repositories.hdfs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.Map;

import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;

public final class HdfsPlugin extends Plugin implements RepositoryPlugin {

    // initialize some problematic classes with elevated privileges
    static {
        SpecialPermission.check();
        AccessController.doPrivileged((PrivilegedAction<Void>) HdfsPlugin::evilHadoopInit);
    }

    @SuppressForbidden(reason = "Needs a security hack for hadoop on windows, until HADOOP-XXXX is fixed")
    private static Void evilHadoopInit() {
        // hack: on Windows, Shell's clinit has a similar problem that on unix,
        // but here we can workaround it for now by setting hadoop home
        // on unix: we still want to set this to something we control, because
        // if the user happens to have HADOOP_HOME in their environment -> checkHadoopHome goes boom
        // TODO: remove THIS when hadoop is fixed
        Path hadoopHome = null;
        String oldValue = null;
        try {
            hadoopHome = Files.createTempDirectory("hadoop").toAbsolutePath();
            oldValue = System.setProperty("hadoop.home.dir", hadoopHome.toString());
            Class.forName("org.apache.hadoop.security.UserGroupInformation");
            Class.forName("org.apache.hadoop.util.StringUtils");
            Class.forName("org.apache.hadoop.util.ShutdownHookManager");
            Class.forName("org.apache.hadoop.conf.Configuration");
            Class.forName("org.apache.hadoop.hdfs.protocol.HdfsConstants");
            Class.forName("org.apache.hadoop.hdfs.protocol.datatransfer.PipelineAck");
        } catch (ClassNotFoundException | IOException e) {
            throw new RuntimeException(e);
        } finally {
            // try to clean up the hack
            if (oldValue == null) {
                System.clearProperty("hadoop.home.dir");
            } else {
                System.setProperty("hadoop.home.dir", oldValue);
            }
            try {
                // try to clean up our temp dir too if we can
                if (hadoopHome != null) {
                    Files.delete(hadoopHome);
                }
            } catch (IOException thisIsBestEffort) {}
        }
        return null;
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(Environment env, NamedXContentRegistry namedXContentRegistry) {
        return Collections.singletonMap("hdfs", (metadata) -> new HdfsRepository(metadata, env, namedXContentRegistry));
    }
}
