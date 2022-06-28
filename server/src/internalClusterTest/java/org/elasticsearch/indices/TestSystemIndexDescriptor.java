/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptyList;
import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

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
        super(
            INDEX_NAME + "*",
            PRIMARY_INDEX_NAME,
            "Test system index",
            getOldMappings(),
            SETTINGS,
            INDEX_NAME,
            0,
            "version",
            "stack",
            SINGLE_MAPPING_NAME,
            Version.CURRENT.minimumCompatibilityVersion(),
            Type.INTERNAL_MANAGED,
            emptyList(),
            emptyList(),
            null,
            false,
            false
        );
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
                builder.startObject(SINGLE_MAPPING_NAME);
                {
                    final Version previousMajor = Version.fromId((Version.CURRENT.major - 1) * 1000000 + 99);

                    builder.startObject("_meta");
                    builder.field("version", previousMajor.toString());
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
            }

            builder.endObject();
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build old .test-index-1 index mappings", e);
        }
    }

    public static String getNewMappings() {
        try {
            final XContentBuilder builder = jsonBuilder();

            builder.startObject();
            {
                builder.startObject(SINGLE_MAPPING_NAME);
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
            }

            builder.endObject();
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build new .test-index-1 index mappings", e);
        }
    }
}
