/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.versionfield;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.BlockLoaderTestCase;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.versionfield.datageneration.VersionStringDataSourceHandler;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class VersionStringFieldBlockLoaderTests extends BlockLoaderTestCase {
    public VersionStringFieldBlockLoaderTests(Params params) {
        super("version", List.of(new VersionStringDataSourceHandler()), params);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Object expected(Map<String, Object> fieldMapping, Object value, TestContext testContext) {
        if (value == null) {
            return null;
        }

        if (value instanceof String s) {
            return convert(s);
        }

        var resultList = ((List<String>) value).stream().map(this::convert).filter(Objects::nonNull).distinct().sorted().toList();
        return maybeFoldList(resultList);
    }

    private BytesRef convert(String value) {
        if (value == null) {
            return null;
        }

        return VersionEncoder.encodeVersion(value).bytesRef;
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return Collections.singletonList(new VersionFieldPlugin(getIndexSettings()));
    }
}
