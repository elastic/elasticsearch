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
package org.elasticsearch.gradle.tool;

import org.gradle.api.Action;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaPluginConvention;
import org.gradle.api.tasks.SourceSetContainer;

import java.util.Optional;

public abstract class Boilerplate {

    public static SourceSetContainer getJavaSourceSets(Project project) {
        return project.getConvention().getPlugin(JavaPluginConvention.class).getSourceSets();
    }

    public static <T> T maybeCreate(NamedDomainObjectContainer<T> collection, String name) {
        return Optional.ofNullable(collection.findByName(name))
            .orElse(collection.create(name));

    }
    public static <T> T maybeCreate(NamedDomainObjectContainer<T> collection, String name, Action<T> action) {
        return Optional.ofNullable(collection.findByName(name))
            .orElseGet(() -> {
                T result = collection.create(name);
                action.execute(result);
                return result;
            });

    }

}
