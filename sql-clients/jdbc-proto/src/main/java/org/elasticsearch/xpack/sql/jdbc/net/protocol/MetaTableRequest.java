/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Locale;

import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.Action;
import org.elasticsearch.xpack.sql.net.client.util.StringUtils;

import static java.lang.String.format;

public class MetaTableRequest extends Request {

    private final String pattern;
    public final String index;
    public final String type;

    public MetaTableRequest(String pattern) {
        super(Action.META_TABLE);

        this.pattern = pattern;
        String[] split = StringUtils.splitToIndexAndType(pattern);

        this.index = split[0];
        this.type = split[1];
    }

    @Override
    public String toString() {
        return format(Locale.ROOT, "MetaTable[index=%s, type=%s]", index, type);
    }

    @Override
    public void encode(DataOutput out) throws IOException {
        out.writeInt(action.value());
        out.writeUTF(pattern);
    }

    public static MetaTableRequest decode(DataInput in) throws IOException {
        String pattern = in.readUTF();
        return new MetaTableRequest(pattern);
    }
}
