/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * A special kind of {@link SystemIndexDescriptor} that can toggle what kind of mappings it
 * expects. A real descriptor is immutable.
 */
public class TestSystemIndexDescriptor extends SystemIndexDescriptor {

    public static final String INDEX_NAME = ".test-index";
    public static final String PRIMARY_INDEX_NAME = INDEX_NAME + "-1";

    public static final AtomicBoolean useNewMappings = new AtomicBoolean(false);

    public static final Settings SETTINGS = Settings.builder()
        .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
        .put(IndexMetadata.INDEX_AUTO_EXPAND_REPLICAS_SETTING.getKey(), "0-1")
        .put(IndexMetadata.SETTING_PRIORITY, Integer.MAX_VALUE)
        .build();

    TestSystemIndexDescriptor() {
        super(INDEX_NAME + "*", PRIMARY_INDEX_NAME, "Test system index", null, SETTINGS, INDEX_NAME, 0, "version", "stack");
    }

    @Override
    public boolean isAutomaticallyManaged() {
        return true;
    }

    @Override
    public String getMappings() {
        return useNewMappings.get() ? getNewMappings() : getOldMappings();
    }

    public static String getOldMappings() {
        try {
            final XContentBuilder builder = jsonBuilder();

            builder.startObject();
            {
                builder.startObject("_meta");
                builder.field("version", Version.CURRENT.previousMajor().toString());
                builder.endObject();

                builder.startObject("properties");
                {
                    builder.startObject("foo");
                    builder.field("type", "text");
                    builder.endObject();
                }
                builder.endObject();
            }

            builder.endObject();
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build .test-index-1 index mappings", e);
        }
    }

    public static String getNewMappings() {
        try {
            final XContentBuilder builder = jsonBuilder();

            builder.startObject();
            {
                builder.startObject("_meta");
                builder.field("version", Version.CURRENT.toString());
                builder.endObject();

                builder.startObject("properties");
                {
                    builder.startObject("bar");
                    builder.field("type", "text");
                    builder.endObject();
                    builder.startObject("foo");
                    builder.field("type", "text");
                    builder.endObject();
                }
                builder.endObject();
            }

            builder.endObject();
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build .test-index-1 index mappings", e);
        }
    }
}
