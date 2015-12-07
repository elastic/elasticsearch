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

package org.elasticsearch.script.expression;

import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptModule;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.text.ParseException;

public class ExpressionPlugin extends Plugin {
    
    // lucene expressions has crazy checks in its clinit for the functions map
    // it violates rules of classloaders to detect accessibility
    // TODO: clean that up
    static {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            @Override
            public Void run() {
                try {
                    JavascriptCompiler.compile("0");
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
                return null;
            }
        });
    }

    @Override
    public String name() {
        return "lang-expression";
    }

    @Override
    public String description() {
        return "Lucene expressions integration for Elasticsearch";
    }

    public void onModule(ScriptModule module) {
        module.addScriptEngine(ExpressionScriptEngineService.class);
    }
}
