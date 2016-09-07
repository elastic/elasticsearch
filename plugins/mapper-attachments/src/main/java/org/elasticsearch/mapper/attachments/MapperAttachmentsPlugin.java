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

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MapperAttachmentsPlugin extends Plugin implements MapperPlugin {

    private static Logger logger = ESLoggerFactory.getLogger("mapper.attachment");
    private static DeprecationLogger deprecationLogger = new DeprecationLogger(logger);

    @Override
    public List<Setting<?>> getSettings() {
        deprecationLogger.deprecated("[mapper-attachments] plugin has been deprecated and will be replaced by [ingest-attachment] plugin.");

        return Arrays.asList(AttachmentMapper.INDEX_ATTACHMENT_DETECT_LANGUAGE_SETTING,
        AttachmentMapper.INDEX_ATTACHMENT_IGNORE_ERRORS_SETTING,
        AttachmentMapper.INDEX_ATTACHMENT_INDEXED_CHARS_SETTING);
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Collections.singletonMap("attachment", new AttachmentMapper.TypeParser());
    }

}
