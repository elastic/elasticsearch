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

package org.elasticsearch.index.query.support;

import org.elasticsearch.index.mapper.ObjectMapper;

import java.util.Deque;
import java.util.LinkedList;

/**
 * During query parsing this keeps track of the current nested level.
 */
public final class NestedScope {

    private final Deque<ObjectMapper> levelStack = new LinkedList<>();

    /**
     * @return For the current nested level returns the object mapper that belongs to that
     */
    public ObjectMapper getObjectMapper() {
        return levelStack.peek();
    }

    /**
     * Sets the new current nested level and pushes old current nested level down the stack returns that level.
     */
    public ObjectMapper nextLevel(ObjectMapper level) {
        ObjectMapper previous = levelStack.peek();
        levelStack.push(level);
        return previous;
    }

    /**
     * Sets the previous nested level as current nested level and removes and returns the current nested level.
     */
    public ObjectMapper previousLevel() {
        return levelStack.pop();
    }

}
