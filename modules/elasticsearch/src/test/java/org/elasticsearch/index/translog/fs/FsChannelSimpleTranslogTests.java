/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.translog.fs;

import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.index.translog.AbstractSimpleTranslogTests;
import org.elasticsearch.index.translog.Translog;
import org.testng.annotations.AfterClass;

import java.io.File;

import static org.elasticsearch.common.settings.ImmutableSettings.Builder.*;

/**
 * @author kimchy (shay.banon)
 */
public class FsChannelSimpleTranslogTests extends AbstractSimpleTranslogTests {

    @Override protected Translog create() {
        return new FsTranslog(shardId, EMPTY_SETTINGS, new File("work/fs-translog"), false);
    }

    @AfterClass public void cleanup() {
        FileSystemUtils.deleteRecursively(new File("work/fs-translog"), true);
    }
}