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

package org.elasticsearch.index.translog;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.BigArrays;

import java.io.IOException;
import java.nio.file.Path;

/**
 *
 */
public class BufferedTranslogTests extends TranslogTests {

    @Override
    protected Translog create(Path path) throws IOException {
        Settings build = Settings.settingsBuilder()
                .put("index.translog.fs.type", TranslogWriter.Type.BUFFERED.name())
                .put("index.translog.fs.buffer_size", 10 + randomInt(128 * 1024), ByteSizeUnit.BYTES)
                .build();
        TranslogConfig translogConfig = new TranslogConfig(shardId, path, build, Translog.Durabilty.REQUEST, BigArrays.NON_RECYCLING_INSTANCE, null);
        return new Translog(translogConfig);
    }
}
