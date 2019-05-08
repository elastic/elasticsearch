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

package org.elasticsearch.repositories.azure;

import org.elasticsearch.common.Strings;

import java.util.Objects;

public class AzureKeyCredentials implements AzureCredentials {
    private final String account;
    private final String key;

    public AzureKeyCredentials(String account, String key) {
        this.account = account;
        this.key = key;
    }

    public String getAccount() {
        return account;
    }

    public String getKey() {
        return key;
    }

    public String buildConnectionString(String endpointSuffix) {
        final StringBuilder connectionStringBuilder = new StringBuilder();
        connectionStringBuilder.append("DefaultEndpointsProtocol=https")
            .append(";AccountName=")
            .append(this.getAccount())
            .append(";AccountKey=")
            .append(this.getKey());
        if (Strings.hasText(endpointSuffix)) {
            connectionStringBuilder.append(";EndpointSuffix=").append(endpointSuffix);
        }
        return connectionStringBuilder.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AzureKeyCredentials that = (AzureKeyCredentials) o;
        return Objects.equals(account, that.account) &&
            Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(account, key);
    }
}
