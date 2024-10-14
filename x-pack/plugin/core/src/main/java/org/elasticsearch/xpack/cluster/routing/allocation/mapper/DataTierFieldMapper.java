/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.cluster.routing.allocation.mapper;

import org.elasticsearch.index.mapper.DataTierFieldType;
import org.elasticsearch.index.mapper.MetadataFieldMapper;

public class DataTierFieldMapper extends MetadataFieldMapper {

    public static final String NAME = DataTierFieldType.NAME;

    public static final String CONTENT_TYPE = DataTierFieldType.CONTENT_TYPE;

    public static final TypeParser PARSER = new FixedTypeParser(c -> new DataTierFieldMapper());

    public DataTierFieldMapper() {
        super(DataTierFieldType.INSTANCE);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
