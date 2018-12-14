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
package org.elasticsearch.client.migration;

import org.elasticsearch.client.Validatable;

import java.util.Objects;

/**
 * A request for performing Upgrade on Index
 * Part of Migration API
 */
public class IndexUpgradeRequest implements Validatable {

    private String index;

    public IndexUpgradeRequest(String index) {
        this.index = index;
    }

    public String index() {
        return index;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexUpgradeRequest request = (IndexUpgradeRequest) o;
        return Objects.equals(index, request.index);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index);
    }
}
