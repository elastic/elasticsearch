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

import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.elasticsearch.common.SuppressForbidden;

import java.io.File;
import java.io.IOException;

/**
 * Extends LFS to improve some operations to keep the security permissions at
 * bay. In particular mkdir is smarter and doesn't have to walk all the file
 * hierarchy but rather only limits itself to the parent/working dir and creates
 * a file only when necessary.
 */
public class TestingFs extends LocalFileSystem {

    private static class ImprovedRawLocalFileSystem extends RawLocalFileSystem {
        @Override
        @SuppressForbidden(reason = "the Hadoop API depends on java.io.File")
        public boolean mkdirs(Path f) throws IOException {
            File wd = pathToFile(getWorkingDirectory());
            File local = pathToFile(f);
            if (wd.equals(local) || local.exists()) {
                return true;
            }
            return mkdirs(f.getParent()) && local.mkdir();
        }
    }

    public TestingFs() {
        super(new ImprovedRawLocalFileSystem());
        // use the build path instead of the starting dir as that one has read permissions
        //setWorkingDirectory(new Path(getClass().getProtectionDomain().getCodeSource().getLocation().toString()));
        setWorkingDirectory(new Path(System.getProperty("java.io.tmpdir")));
    }
}
