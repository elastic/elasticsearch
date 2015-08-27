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

package org.elasticsearch.plugin.mappingtransform;

import org.elasticsearch.index.mapper.MapperServiceModule;
import org.elasticsearch.plugins.Plugin;

/**
 * Plugin handling scripted "transform"s that allow you to index that data
 * differently than it is stored in _source. This used to be a core feature of
 * Elasticsearch but it makes debugging baffling sometimes. Use with great care.
 * You are better off if you just think of these transform scripts as
 * <code>copy_to</code> on steroids.
 */
public class MappingTransformPlugin extends Plugin {
    public static final String NAME = "mapping-transform";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String description() {
        return "Elasticsearch Mapping-Transform Plugin";
    }

    public void onModule(MapperServiceModule module) {
        module.addRootParser("transform", TransformParser.class);
    }
}
