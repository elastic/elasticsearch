/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.sql.action.BasicFormatter;

import java.io.IOException;

public record FormatterState(BasicFormatter formatter) implements Writeable {

    public FormatterState(StreamInput in) throws IOException {
        this(in.<BasicFormatter>readOptionalWriteable(BasicFormatter::new));
    }

    public static FormatterState EMPTY = new FormatterState((BasicFormatter) null);

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(formatter);
    }

}
