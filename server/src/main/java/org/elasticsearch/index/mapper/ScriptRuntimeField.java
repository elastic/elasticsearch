/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public abstract class ScriptRuntimeField implements RuntimeField {

    protected final String name;
    protected final ToXContent toXContent;

    public ScriptRuntimeField(String name, ToXContent toXContent) {
        this.name = name;
        this.toXContent = toXContent;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public final Collection<MappedFieldType> asMappedFieldTypes() {
        return Collections.singleton(asMappedFieldType());
    }

    protected abstract MappedFieldType asMappedFieldType();

    @Override
    public final void doXContentBody(XContentBuilder builder, Params params) throws IOException {
        toXContent.toXContent(builder, params);
    }
}
