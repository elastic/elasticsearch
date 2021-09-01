/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;

public class ExternalMetadataMapper extends MetadataFieldMapper {

    static final String CONTENT_TYPE = "_external_root";
    static final String FIELD_NAME = "_is_external";
    static final String FIELD_VALUE = "true";

    protected ExternalMetadataMapper() {
        super(new BooleanFieldMapper.BooleanFieldType(FIELD_NAME));
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public void postParse(DocumentParserContext context) {
        context.doc().add(new StringField(FIELD_NAME, FIELD_VALUE, Store.YES));
    }

    public static final TypeParser PARSER = new FixedTypeParser(c -> new ExternalMetadataMapper());

}
