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

package org.elasticsearch.client.advanced.delete;

import org.elasticsearch.client.advanced.Strings;
import org.elasticsearch.client.advanced.RestRequest;

/**
 * Delete a document request
 */
public class DeleteRestRequest implements RestRequest {

    private String index;
    private String type;
    private String id;

    private DeleteRestRequest(String index, String type, String id) {
        this.index = index;
        this.type = type;
        this.id = id;
    }

    /**
     * @return Index name
     */
    public String getIndex() {
        return index;
    }

    /**
     * @return Type
     */
    public String getType() {
        return type;
    }

    /**
     * @return Document Id
     */
    public String getId() {
        return id;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String index;
        private String type;
        private String id;

        /**
         * @param index Index name
         */
        public Builder setIndex(String index) {
            this.index = index;
            return this;
        }

        /**
         * @param type Type
         */
        public Builder setType(String type) {
            this.type = type;
            return this;
        }

        /**
         * @param id Document Id
         */
        public Builder setId(String id) {
            this.id = id;
            return this;
        }

        public DeleteRestRequest build() {
            return new DeleteRestRequest(index, type, id);
        }
    }

    /**
     * Validation rules: checked before executing the request
     */
    public void validate() {
        if (Strings.hasText(index) == false) {
            throw new IllegalArgumentException("Index can not be null");
        }
        if (Strings.hasText(type) == false) {
            throw new IllegalArgumentException("Type can not be null");
        }
        if (Strings.hasText(id) == false) {
            throw new IllegalArgumentException("Id can not be null");
        }
    }
}
