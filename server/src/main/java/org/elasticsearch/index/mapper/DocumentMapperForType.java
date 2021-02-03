/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

public class DocumentMapperForType {
    private final DocumentMapper documentMapper;
    private final Mapping mapping;

    public DocumentMapperForType(DocumentMapper documentMapper, Mapping mapping) {
        this.mapping = mapping;
        this.documentMapper = documentMapper;
    }

    public DocumentMapper getDocumentMapper() {
        return documentMapper;
    }

    public Mapping getMapping() {
        return mapping;
    }
}
