/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import org.elasticsearch.xpack.sql.protocol.shared.SqlDataInput;
import org.elasticsearch.xpack.sql.protocol.shared.SqlDataOutput;

import java.io.IOException;

public interface Payload {

    void readFrom(SqlDataInput in) throws IOException;

    void writeTo(SqlDataOutput out) throws IOException;
}
