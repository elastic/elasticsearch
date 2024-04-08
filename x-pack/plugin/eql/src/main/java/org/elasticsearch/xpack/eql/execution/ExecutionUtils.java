/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;

import java.io.IOException;
import java.util.List;

public class ExecutionUtils {

    private static NamedWriteableRegistry registry = new NamedWriteableRegistry(
        new SearchModule(Settings.EMPTY, List.of()).getNamedWriteables()
    );

    /*
     * Not a great way of getting a copy of a SearchSourceBuilder
     */
    public static SearchSourceBuilder copySource(SearchSourceBuilder source) {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            source.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), registry)) {
                return new SearchSourceBuilder(in);
            }
        } catch (IOException e) {
            throw new EqlIllegalArgumentException("Error copying search source", e);
        }
    }

}
