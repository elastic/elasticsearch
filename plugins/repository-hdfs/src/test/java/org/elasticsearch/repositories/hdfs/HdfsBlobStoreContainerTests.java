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

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.test.ESTestCase;

import javax.security.auth.Subject;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;

import static org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase.randomBytes;
import static org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase.readBlobFully;
import static org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase.writeBlob;

@ThreadLeakFilters(filters = {HdfsClientThreadLeakFilter.class})
public class HdfsBlobStoreContainerTests extends ESTestCase {

    private FileContext createTestContext() {
        FileContext fileContext;
        try {
            fileContext = AccessController.doPrivileged((PrivilegedExceptionAction<FileContext>)
                () -> createContext(new URI("hdfs:///")));
        } catch (PrivilegedActionException e) {
            throw new RuntimeException(e.getCause());
        }
        return fileContext;
    }

    @SuppressForbidden(reason = "lesser of two evils (the other being a bunch of JNI/classloader nightmares)")
    private FileContext createContext(URI uri) {
        // mirrors HdfsRepository.java behaviour
        Configuration cfg = new Configuration(true);
        cfg.setClassLoader(HdfsRepository.class.getClassLoader());
        cfg.reloadConfiguration();

        Constructor<?> ctor;
        Subject subject;

        try {
            Class<?> clazz = Class.forName("org.apache.hadoop.security.User");
            ctor = clazz.getConstructor(String.class);
            ctor.setAccessible(true);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }

        try {
            Principal principal = (Principal) ctor.newInstance(System.getProperty("user.name"));
            subject = new Subject(false, Collections.singleton(principal),
                    Collections.emptySet(), Collections.emptySet());
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }

        // disable file system cache
        cfg.setBoolean("fs.hdfs.impl.disable.cache", true);

        // set file system to TestingFs to avoid a bunch of security
        // checks, similar to what is done in HdfsTests.java
        cfg.set("fs.AbstractFileSystem." + uri.getScheme() + ".impl", TestingFs.class.getName());

        // create the FileContext with our user
        return Subject.doAs(subject, (PrivilegedAction<FileContext>) () -> {
            try {
                TestingFs fs = (TestingFs) AbstractFileSystem.get(uri, cfg);
                return FileContext.getFileContext(fs, cfg);
            } catch (UnsupportedFileSystemException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void testReadOnly() throws Exception {
        FileContext fileContext = createTestContext();
        // Constructor will not create dir if read only
        HdfsBlobStore hdfsBlobStore = new HdfsBlobStore(fileContext, "dir", 1024, true);
        FileContext.Util util = fileContext.util();
        Path root = fileContext.makeQualified(new Path("dir"));
        assertFalse(util.exists(root));
        BlobPath blobPath = BlobPath.cleanPath().add("path");

        // blobContainer() will not create path if read only
        hdfsBlobStore.blobContainer(blobPath);
        Path hdfsPath = root;
        for (String p : blobPath) {
            hdfsPath = new Path(hdfsPath, p);
        }
        assertFalse(util.exists(hdfsPath));

        // if not read only, directory will be created
        hdfsBlobStore = new HdfsBlobStore(fileContext, "dir", 1024, false);
        assertTrue(util.exists(root));
        BlobContainer container = hdfsBlobStore.blobContainer(blobPath);
        assertTrue(util.exists(hdfsPath));

        byte[] data = randomBytes(randomIntBetween(10, scaledRandomIntBetween(1024, 1 << 16)));
        writeBlob(container, "foo", new BytesArray(data), randomBoolean());
        assertArrayEquals(readBlobFully(container, "foo", data.length), data);
        assertTrue(BlobStoreTestUtil.blobExists(container, "foo"));
    }
}
