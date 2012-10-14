/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.search.group;

import java.util.List;
import java.util.Map;

/**
 * Groups of search action.
 *
 *
 */
public interface Groups extends Iterable<Group> {

    /**
     * The list of {@link Group}s.
     */
    List<Group> groups();

    /**
     * Returns the {@link Group}s keyed by group name.
     */
//    Map<String, Group> getGroups();

    /**
     * Returns the {@link Group}s keyed by group name.
     */
//    Map<String, Group> groupsAsMap();

    /**
     * Returns the group by name already casted to the specified type.
     */
    <T extends Group> T group(Class<T> groupType, String name);

    /**
     * A group of the specified name.
     */
    <T extends Group> T group(String name);
}
