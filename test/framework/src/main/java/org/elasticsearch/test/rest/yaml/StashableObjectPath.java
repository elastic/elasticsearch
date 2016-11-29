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

package org.elasticsearch.test.rest.yaml;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.xcontent.ObjectPath;

import java.io.IOException;;

public class StashableObjectPath {

    private ObjectPath objectPath;

    public StashableObjectPath(ObjectPath objectPath) {
        this.objectPath = objectPath;
    }

    public Object evaluate(String path) {
        return evaluate(path, Stash.EMPTY);
    }

    /**
     * Returns the object corresponding to the provided path if present, null otherwise
     */
    public Object evaluate(String path, Stash stash) {
        String[] parts = ObjectPath.parsePath(path);
        Object object = this.objectPath.getObject();
        for (String part : parts) {
            if (stash.containsStashedValue(part)) {
                try {
                    part = stash.getValue(part).toString();
                } catch (IOException e) {
                    throw new ElasticsearchException(e);
                }
            }
            object = ObjectPath.evaluate(part, object);
            if (object == null) {
                return null;
            }
        }
        return object;
    }
}
