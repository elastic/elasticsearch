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

package org.elasticsearch.mapper.attachments;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

public abstract class AttachmentUnitTestCase extends ESTestCase {

    protected Settings testSettings;

    protected static IndicesModule getIndicesModuleWithRegisteredAttachmentMapper() {
        IndicesModule indicesModule = new IndicesModule(new NamedWriteableRegistry());
        indicesModule.registerMapper(AttachmentMapper.CONTENT_TYPE, new AttachmentMapper.TypeParser());
        return indicesModule;
    }

    @Before
    public void createSettings() throws Exception {
      testSettings = Settings.builder()
                             .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                             .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT.id)
                             .build();
    }
}
