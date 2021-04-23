/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Collections;
import java.util.Map;

public class ExternalMapperPlugin extends Plugin implements MapperPlugin {

    public static final String EXTERNAL = "external";
    public static final String EXTERNAL_BIS = "external_bis";
    public static final String EXTERNAL_UPPER = "external_upper";

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Map.of(
                EXTERNAL, ExternalMapper.parser(EXTERNAL, "foo"),
                EXTERNAL_BIS, ExternalMapper.parser(EXTERNAL_BIS, "bar"),
                EXTERNAL_UPPER, ExternalMapper.parser(EXTERNAL_UPPER, "FOO BAR"),
                FakeStringFieldMapper.CONTENT_TYPE, FakeStringFieldMapper.PARSER);
    }

    @Override
    public Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers() {
        return Collections.singletonMap(ExternalMetadataMapper.CONTENT_TYPE, ExternalMetadataMapper.PARSER);
    }

}
