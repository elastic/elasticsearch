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
import java.lang.reflect.Constructor;
import java.net.URI;
import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import javax.security.auth.Subject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.snapshots.IndexShardRepository;
import org.elasticsearch.repositories.RepositoryName;
import org.elasticsearch.repositories.RepositorySettings;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

public final class HdfsRepository extends BlobStoreRepository {

    private final BlobPath basePath = BlobPath.cleanPath();
    private final RepositorySettings repositorySettings;
    private final ByteSizeValue chunkSize;
    private final boolean compress;

    private HdfsBlobStore blobStore;

    // buffer size passed to HDFS read/write methods
    // TODO: why 100KB?
    private static final ByteSizeValue DEFAULT_BUFFER_SIZE = new ByteSizeValue(100, ByteSizeUnit.KB);

    @Inject
    public HdfsRepository(RepositoryName name, RepositorySettings repositorySettings, IndexShardRepository indexShardRepository) throws IOException {
        super(name.getName(), repositorySettings, indexShardRepository);
        this.repositorySettings = repositorySettings;

        this.chunkSize = repositorySettings.settings().getAsBytesSize("chunk_size", null);
        this.compress = repositorySettings.settings().getAsBoolean("compress", false);
    }

    @Override
    protected void doStart() {
        String uriSetting = repositorySettings.settings().get("uri");
        if (Strings.hasText(uriSetting) == false) {
            throw new IllegalArgumentException("No 'uri' defined for hdfs snapshot/restore");
        }
        URI uri = URI.create(uriSetting);
        if ("hdfs".equalsIgnoreCase(uri.getScheme()) == false) {
            throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Invalid scheme [%s] specified in uri [%s]; only 'hdfs' uri allowed for hdfs snapshot/restore", uri.getScheme(), uriSetting));
        }
        if (Strings.hasLength(uri.getPath()) && uri.getPath().equals("/") == false) {
            throw new IllegalArgumentException(String.format(Locale.ROOT,
                    "Use 'path' option to specify a path [%s], not the uri [%s] for hdfs snapshot/restore", uri.getPath(), uriSetting));
        }

        String pathSetting = repositorySettings.settings().get("path");
        // get configuration
        if (pathSetting == null) {
            throw new IllegalArgumentException("No 'path' defined for hdfs snapshot/restore");
        }
        
        int bufferSize = repositorySettings.settings().getAsBytesSize("buffer_size", DEFAULT_BUFFER_SIZE).bytesAsInt();

        try {
            // initialize our filecontext
            SecurityManager sm = System.getSecurityManager();
            if (sm != null) {
                sm.checkPermission(new SpecialPermission());
            }
            FileContext fileContext = AccessController.doPrivileged(new PrivilegedAction<FileContext>() {
                @Override
                public FileContext run() {
                    return createContext(uri, repositorySettings);
                }
            });
            blobStore = new HdfsBlobStore(fileContext, pathSetting, bufferSize);
            logger.debug("Using file-system [{}] for URI [{}], path [{}]", fileContext.getDefaultFileSystem(), fileContext.getDefaultFileSystem().getUri(), pathSetting);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException(String.format(Locale.ROOT, "Cannot create HDFS repository for uri [%s]", uri), e);
        }
        super.doStart();
    }
    
    // create hadoop filecontext
    @SuppressForbidden(reason = "lesser of two evils (the other being a bunch of JNI/classloader nightmares)")
    private static FileContext createContext(URI uri, RepositorySettings repositorySettings)  {
        Configuration cfg = new Configuration(repositorySettings.settings().getAsBoolean("load_defaults", true));
        cfg.setClassLoader(HdfsRepository.class.getClassLoader());
        cfg.reloadConfiguration();

        Map<String, String> map = repositorySettings.settings().getByPrefix("conf.").getAsMap();
        for (Entry<String, String> entry : map.entrySet()) {
            cfg.set(entry.getKey(), entry.getValue());
        }

        // create a hadoop user. if we want some auth, it must be done different anyway, and tested.
        Subject subject;
        try {
            Class<?> clazz = Class.forName("org.apache.hadoop.security.User");
            Constructor<?> ctor = clazz.getConstructor(String.class);
            ctor.setAccessible(true);
            Principal principal = (Principal) ctor.newInstance(System.getProperty("user.name"));
            subject = new Subject(false, Collections.singleton(principal), Collections.emptySet(), Collections.emptySet());
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }

        // disable FS cache
        cfg.setBoolean("fs.hdfs.impl.disable.cache", true);

        // create the filecontext with our user
        return Subject.doAs(subject, new PrivilegedAction<FileContext>() {
            @Override
            public FileContext run() {
                try {
                    AbstractFileSystem fs = AbstractFileSystem.get(uri, cfg);
                    return FileContext.getFileContext(fs, cfg);
                } catch (UnsupportedFileSystemException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    @Override
    protected BlobStore blobStore() {
        return blobStore;
    }

    @Override
    protected BlobPath basePath() {
        return basePath;
    }

    @Override
    protected boolean isCompress() {
        return compress;
    }

    @Override
    protected ByteSizeValue chunkSize() {
        return chunkSize;
    }
}
