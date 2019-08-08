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
import org.gradle.api.Task;
import org.gradle.api.UnknownTaskException;
import org.gradle.api.plugins.JavaPluginConvention;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskContainer;
import org.gradle.api.tasks.TaskProvider;

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

    public static <T extends Task> TaskProvider<T> maybeRegister(TaskContainer tasks, String name, Class<T> clazz, Action<T> action) {
        try {
            return tasks.named(name, clazz);
        } catch (UnknownTaskException e) {
            return tasks.register(name, clazz, action);
        }
    }

    public static void maybeConfigure(TaskContainer tasks, String name, Action<? super Task> config) {
        TaskProvider<?> task;
        try {
            task = tasks.named(name);
        } catch (UnknownTaskException e) {
            return;
        }

        task.configure(config);
    }

    public static <T extends Task> void maybeConfigure(
        TaskContainer tasks, String name,
        Class<? extends T> type,
        Action<? super T> config
    ) {
        tasks.withType(type).configureEach(task -> {
            if (task.getName().equals(name)) {
                config.execute(task);
            }
        });
    }

    public static TaskProvider<?> findByName(TaskContainer tasks, String name) {
        TaskProvider<?> task;
        try {
            task = tasks.named(name);
        } catch (UnknownTaskException e) {
            return null;
        }

        return task;
    }
}
