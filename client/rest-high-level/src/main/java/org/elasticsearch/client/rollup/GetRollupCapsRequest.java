/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.rollup;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;

import java.util.Objects;

public class GetRollupCapsRequest implements Validatable {

    private final String indexPattern;

    public GetRollupCapsRequest(final String indexPattern) {
        if (Strings.isNullOrEmpty(indexPattern) || indexPattern.equals("*")) {
            this.indexPattern = Metadata.ALL;
        } else {
            this.indexPattern = indexPattern;
        }
    }

    public String getIndexPattern() {
        return indexPattern;
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexPattern);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        GetRollupCapsRequest other = (GetRollupCapsRequest) obj;
        return Objects.equals(indexPattern, other.indexPattern);
    }
}
