/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.Action;

public abstract class DataResponse extends Response {

    // there is no type for this field since depending on where it is used, the data is represented differently
    // on the server it is a RowSetCursor (before being sent to the wire), on the client a Page (after being read from the wire)
    public final Object data;

    public DataResponse(Action action, Object data) {
        super(action);
        this.data = data;
    }
}
