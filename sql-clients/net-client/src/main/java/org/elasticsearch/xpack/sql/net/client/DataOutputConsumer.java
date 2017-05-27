/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.net.client;

import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Consumer;

public interface DataOutputConsumer extends Consumer<DataOutput> {

    default void accept(DataOutput out) {
        try {
            acceptThrows(out);
        } catch (IOException ex) {
            throw new ClientException(ex, "Cannot write request");
        }
    }

    void acceptThrows(DataOutput out) throws IOException;
}