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

package org.elasticsearch.client.indexlifecycle;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.TimedRequest;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.common.Strings;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * The request object used by the Explain Lifecycle API.
 *
 * Multiple indices may be queried in the same request using the
 * {@link #indices(String...)} method
 */
public class ExplainLifecycleRequest extends TimedRequest {

    private String[] indices = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();

    public ExplainLifecycleRequest() {
        super();
    }

    public ExplainLifecycleRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    public String[] indices() {
        return indices;
    }

    public ExplainLifecycleRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }
    
    @Override
    public Optional<ValidationException> validate() {
        return Optional.empty();
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(indices()), indicesOptions());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        ExplainLifecycleRequest other = (ExplainLifecycleRequest) obj;
        return Objects.deepEquals(indices(), other.indices()) &&
                Objects.equals(indicesOptions(), other.indicesOptions());
    }

    @Override
    public String toString() {
        return "ExplainLifecycleRequest [indices()=" + Arrays.toString(indices()) + ", indicesOptions()=" + indicesOptions() + "]";
    }

}
