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

package org.elasticsearch.common.inject;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;

import java.lang.reflect.Constructor;

/**
 *
 */
public class Modules {

    public static Module createModule(String moduleClass, Settings settings) throws ClassNotFoundException {
        return createModule((Class<? extends Module>) settings.getClassLoader().loadClass(moduleClass), settings);
    }

    public static Module createModule(Class<? extends Module> moduleClass, @Nullable Settings settings) {
        Constructor<? extends Module> constructor;
        try {
            constructor = moduleClass.getConstructor(Settings.class);
            try {
                return constructor.newInstance(settings);
            } catch (Exception e) {
                throw new ElasticsearchException("Failed to create module [" + moduleClass + "]", e);
            }
        } catch (NoSuchMethodException e) {
            try {
                constructor = moduleClass.getConstructor();
                try {
                    return constructor.newInstance();
                } catch (Exception e1) {
                    throw new ElasticsearchException("Failed to create module [" + moduleClass + "]", e);
                }
            } catch (NoSuchMethodException e1) {
                throw new ElasticsearchException("No constructor for [" + moduleClass + "]");
            }
        }
    }

    public static void processModules(Iterable<Module> modules) {
        for (Module module : modules) {
            if (module instanceof PreProcessModule) {
                for (Module module1 : modules) {
                    ((PreProcessModule) module).processModule(module1);
                }
            }
        }
    }
}
