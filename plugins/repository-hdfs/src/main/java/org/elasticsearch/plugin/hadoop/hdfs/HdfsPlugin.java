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

import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardRepository;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesModule;
import org.elasticsearch.repositories.Repository;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

//
// Note this plugin is somewhat special as Hadoop itself loads a number of libraries and thus requires a number of permissions to run even in client mode.
// This poses two problems:
// - Hadoop itself comes with tons of jars, many providing the same classes across packages. In particular Hadoop 2 provides package annotations in the same
//   package across jars which trips JarHell. Thus, to allow Hadoop jars to load, the plugin uses a dedicated CL which picks them up from the hadoop-libs folder.
// - The issue though with using a different CL is that it picks up the jars from a different location / codeBase and thus it does not fall under the plugin
//   permissions. In other words, the plugin permissions don't apply to the hadoop libraries.
//   There are different approaches here:
//      - implement a custom classloader that loads the jars but 'lies' about the codesource. It is doable but since URLClassLoader is locked down, one would
//        would have to implement the whole jar opening and loading from it. Not impossible but still fairly low-level.
//        Further more, even if the code has the proper credentials, it needs to use the proper Privileged blocks to use its full permissions which does not
//        happen in the Hadoop code base.
//      - use a different Policy. Works but the Policy is JVM wide and thus the code needs to be quite efficient - quite a bit impact to cover just some plugin
//        libraries
//      - use a DomainCombiner. This doesn't change the semantics (it's clear where the code is loaded from, etc..) however it gives us a scoped, fine-grained
//        callback on handling the permission intersection for secured calls. Note that DC works only in the current PAC call - the moment another PA is used,
//        the domain combiner is going to be ignored (unless the caller specifically uses it). Due to its scoped impact and official Java support, this approach
//        was used.

// ClassLoading info
// - package plugin.hadoop.hdfs is part of the plugin
// - all the other packages are assumed to be in the nested Hadoop CL.

// Code
public class HdfsPlugin extends Plugin {

    @Override
    public String name() {
        return "repository-hdfs";
    }

    @Override
    public String description() {
        return "HDFS Repository Plugin";
    }

    @SuppressWarnings("unchecked")
    public void onModule(RepositoriesModule repositoriesModule) {
        String baseLib = Utils.detectLibFolder();
        List<URL> cp = getHadoopClassLoaderPath(baseLib);

        ClassLoader hadoopCL = URLClassLoader.newInstance(cp.toArray(new URL[cp.size()]), getClass().getClassLoader());

        Class<? extends Repository> repository = null;
        try {
            repository = (Class<? extends Repository>) hadoopCL.loadClass("org.elasticsearch.repositories.hdfs.HdfsRepository");
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalStateException("Cannot load plugin class; is the plugin class setup correctly?", cnfe);
        }

        repositoriesModule.registerRepository("hdfs", repository, BlobStoreIndexShardRepository.class);
        Loggers.getLogger(HdfsPlugin.class).info("Loaded Hadoop [{}] libraries from {}", getHadoopVersion(hadoopCL), baseLib);
    }

    protected List<URL> getHadoopClassLoaderPath(String baseLib) {
        List<URL> cp = new ArrayList<>();
        // add plugin internal jar
        discoverJars(createURI(baseLib, "internal-libs"), cp, false);
        // add Hadoop jars
        discoverJars(createURI(baseLib, "hadoop-libs"), cp, true);
        return cp;
    }

    private String getHadoopVersion(ClassLoader hadoopCL) {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            // unprivileged code such as scripts do not have SpecialPermission
            sm.checkPermission(new SpecialPermission());
        }

        return AccessController.doPrivileged(new PrivilegedAction<String>() {
            @Override
            public String run() {
                // Hadoop 2 relies on TCCL to determine the version
                ClassLoader tccl = Thread.currentThread().getContextClassLoader();
                try {
                    Thread.currentThread().setContextClassLoader(hadoopCL);
                    return doGetHadoopVersion(hadoopCL);
                } finally {
                    Thread.currentThread().setContextClassLoader(tccl);
                }
            }
        }, Utils.hadoopACC());
    }

    private String doGetHadoopVersion(ClassLoader hadoopCL) {
        String version = "Unknown";

        Class<?> clz = null;
        try {
            clz = hadoopCL.loadClass("org.apache.hadoop.util.VersionInfo");
        } catch (ClassNotFoundException cnfe) {
            // unknown
        }
        if (clz != null) {
            try {
                Method method = clz.getMethod("getVersion");
                version = method.invoke(null).toString();
            } catch (Exception ex) {
                // class has changed, ignore
            }
        }

        return version;
    }

    private URI createURI(String base, String suffix) {
        String location = base + suffix;
        try {
            return new URI(location);
        } catch (URISyntaxException ex) {
            throw new IllegalStateException(String.format(Locale.ROOT, "Cannot detect plugin folder; [%s] seems invalid", location), ex);
        }
    }

    @SuppressForbidden(reason = "discover nested jar")
    private void discoverJars(URI libPath, List<URL> cp, boolean optional) {
        try {
            Path[] jars = FileSystemUtils.files(PathUtils.get(libPath), "*.jar");

            for (Path path : jars) {
                cp.add(path.toUri().toURL());
            }
        } catch (IOException ex) {
            if (!optional) {
                throw new IllegalStateException("Cannot compute plugin classpath", ex);
            }
        }
    }
}
