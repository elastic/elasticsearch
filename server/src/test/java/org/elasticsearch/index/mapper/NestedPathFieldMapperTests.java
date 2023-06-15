/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.hamcrest.Matchers.empty;

public class NestedPathFieldMapperTests extends MetadataMapperTestCase {

    @Override
    protected String fieldName() {
        return NestedPathFieldMapper.NAME;
    }

    @Override
    protected boolean isSupportedOn(IndexVersion version) {
        return version.onOrAfter(IndexVersion.V_8_0_0);
    }

    @Override
    protected boolean isConfigurable() {
        return false;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {}

    public void testDefaults() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        ParsedDocument document = mapper.parse(new SourceToParse("id", new BytesArray("{}"), XContentType.JSON));
        assertThat(document.rootDoc().getFields(NestedPathFieldMapper.NAME), empty());
    }
}
