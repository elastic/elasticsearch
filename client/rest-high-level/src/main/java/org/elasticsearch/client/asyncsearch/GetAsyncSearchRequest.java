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


package org.elasticsearch.client.asyncsearch;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Objects;
import java.util.Optional;

public class GetAsyncSearchRequest implements Validatable {

    private TimeValue waitForCompletion;
    private TimeValue keepAlive;

    private final String id;

    public GetAsyncSearchRequest(String id) {
        this.id = id;
    }

    public String getId() {
        return this.id;
    }

    public TimeValue getWaitForCompletion() {
        return waitForCompletion;
    }

    public void setWaitForCompletion(TimeValue waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
    }

    public TimeValue getKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(TimeValue keepAlive) {
        this.keepAlive = keepAlive;
    }

    @Override
    public Optional<ValidationException> validate() {
        return Optional.empty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GetAsyncSearchRequest request = (GetAsyncSearchRequest) o;
        return Objects.equals(getId(), request.getId())
                && Objects.equals(getKeepAlive(), request.getKeepAlive())
                && Objects.equals(getWaitForCompletion(), request.getWaitForCompletion());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), getKeepAlive(), getWaitForCompletion());
    }
}
