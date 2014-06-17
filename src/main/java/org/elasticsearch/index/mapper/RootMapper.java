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

package org.elasticsearch.index.mapper;

import java.io.IOException;


/**
 * A mapper that exists only as a mapper within a root object.
 */
public interface RootMapper extends Mapper {

    void preParse(ParseContext context) throws IOException;

    void postParse(ParseContext context) throws IOException;

    /**
     * Should the mapper be included in the root {@link org.elasticsearch.index.mapper.object.ObjectMapper}.
     */
    boolean includeInObject();

}
