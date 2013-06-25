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

package org.elasticsearch.plugins;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

/**
 * An invocation handler that sets the current Thread classloader to be the plugin one
 * during its invocation. 
 * @author Laurent Broudoux
 */
public class PluginInvocationHandler implements InvocationHandler {

    private final ESLogger logger = Loggers.getLogger(PluginInvocationHandler.class);
    
    private Plugin targetPlugin;
    
    
    public PluginInvocationHandler(Plugin targetPlugin){
        this.targetPlugin = targetPlugin;
        logger.trace("PluginInvocationHandler built for {}", targetPlugin.getClass().getName());
    }
    
    @Override
    public Object invoke(Object target, Method method, Object[] args) throws Throwable {
        ClassLoader clBackup = Thread.currentThread().getContextClassLoader();
        logger.trace("ClassLoader before plugin invocation is {}", clBackup);
        try{
            // Change thread context classloader to the plugin classloader. 
            Thread.currentThread().setContextClassLoader(targetPlugin.getClass().getClassLoader());
            logger.trace("ClassLoader is set to {0} for plugin invocation", targetPlugin.getClass().getClassLoader());
            return method.invoke(targetPlugin, args);
        } finally {
            // Restore thread context classloader to the original one. 
            Thread.currentThread().setContextClassLoader(clBackup);
        }
    }

}
