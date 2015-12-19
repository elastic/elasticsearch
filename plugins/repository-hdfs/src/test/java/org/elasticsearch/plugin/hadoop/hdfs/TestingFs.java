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
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Extends LFS to improve some operations to keep the security permissions at
 * bay. In particular it never tries to execute!
 */
public class TestingFs extends DelegateToFileSystem {

    private static class ImprovedRawLocalFileSystem extends RawLocalFileSystem {
        @Override
        public Path getInitialWorkingDirectory() {
            // sets working dir to a tmp dir for testing
            return new Path(LuceneTestCase.createTempDir().toString());
        }
        
        @Override
        public void setPermission(Path p, FsPermission permission) {
           // no execution, thank you very much!
        }
    }

    public TestingFs(URI uri, Configuration configuration) throws URISyntaxException, IOException {
        super(URI.create("file:///"), new ImprovedRawLocalFileSystem(), configuration, "file", false);
    }

    @Override
    public void checkPath(Path path) {
      // we do evil stuff, we admit it.
    }
}
