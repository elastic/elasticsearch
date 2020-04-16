/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
