/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class MlInfoResponse implements Validatable {
    private final Map<String, Object> info;

    private MlInfoResponse(Map<String, Object> info) {
        this.info = info;
    }

    public Map<String, Object> getInfo() {
        return info;
    }

    public static MlInfoResponse fromXContent(XContentParser parser) throws IOException {
        Map<String, Object> info = parser.map();
        return new MlInfoResponse(info);
    }

    @Override
    public int hashCode() {
        return Objects.hash(info);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        MlInfoResponse other = (MlInfoResponse) obj;
        return Objects.equals(info, other.info);
    }
}
