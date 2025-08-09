/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

public class ESONFlat {

    private final List<ESONEntry> keyArray;
    private final ESONSource.Values values;

    public ESONFlat(List<ESONEntry> keyArray, ESONSource.Values values) {
        this.keyArray = keyArray;
        this.values = values;
    }

    public void toBytes(StreamOutput output) throws IOException {
        for (ESONEntry entry : keyArray) {

        }
        output.writeBytesReference(values.data());

    }

    public void fromBytes() {

    }
}
