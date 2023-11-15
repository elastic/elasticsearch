/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.countedkeyword;

import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class CountedKeywordFieldMapperTests extends MapperTestCase {
    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return Collections.singletonList(new CountedKeywordMapperPlugin());
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", CountedKeywordFieldMapper.CONTENT_TYPE);
    }

    @Override
    protected Object getSampleValueForDocument() {
        return new String[] { "a", "a", "b", "c" };
    }

    @Override
    protected Object getSampleValueForQuery() {
        return "b";
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return false;
    }

    @Override
    protected boolean supportsStoredFields() {
        return false;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) {
        // Nothing to do
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        return randomBoolean() ? null : randomAlphaOfLengthBetween(1, 10);
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        throw new AssumptionViolatedException("not supported");
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }
}
